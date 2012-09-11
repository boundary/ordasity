//
// Copyright 2011-2012, Boundary
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package com.boundary.ordasity

import com.codahale.jerkson.Json._
import com.codahale.logula.Logging
import com.yammer.metrics.scala.{Meter, Instrumented}
import java.lang.management.ManagementFactory
import javax.management.ObjectName

import java.util.{HashMap, Map}
import scala.collection.JavaConversions._
import org.cliffc.high_scale_lib.NonBlockingHashSet
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import java.net.InetSocketAddress
import org.apache.zookeeper.KeeperException.NoNodeException
import com.twitter.common.quantity.{Time, Amount}
import com.twitter.common.zookeeper.{ZooKeeperMap => ZKMap, ZooKeeperClient}

import listeners._
import balancing.{CountBalancingPolicy, MeteredBalancingPolicy}
import org.apache.zookeeper.{WatchedEvent, Watcher}
import org.apache.zookeeper.Watcher.Event.KeeperState
import java.util.concurrent.{TimeoutException, TimeUnit, ScheduledFuture, ScheduledThreadPoolExecutor}

trait ClusterMBean {
  def join() : String
  def shutdown()
  def rebalance()
}

class Cluster(val name: String, val listener: Listener, config: ClusterConfig)
    extends ClusterMBean with Logging with Instrumented {
  var myNodeID = config.nodeId
  val watchesRegistered = new AtomicBoolean(false)
  val initialized = new AtomicBoolean(false)
  val connected = new AtomicBoolean(false)

  // Register Ordasity with JMX for management / instrumentation.
  ManagementFactory.getPlatformMBeanServer.registerMBean(
    this, new ObjectName(name + ":" + "name=Cluster"))

  // Cluster, node, and work unit state
  var nodes : Map[String, NodeInfo] = null
  val myWorkUnits = new NonBlockingHashSet[String]
  var allWorkUnits : Map[String, String] = null
  var workUnitMap : Map[String, String] = null
  var handoffRequests : Map[String, String] = null
  var handoffResults : Map[String, String] = null
  val claimedForHandoff = new NonBlockingHashSet[String]
  var loadMap : Map[String, Double] = null
  val workUnitsPeggedToMe = new NonBlockingHashSet[String]

  var balancingPolicy = {
    if (config.useSmartBalancing)
      new MeteredBalancingPolicy(this, config).init()
    else
      new CountBalancingPolicy(this, config).init()
  }

  // Scheduled executions
  val pool = new AtomicReference[ScheduledThreadPoolExecutor](new ScheduledThreadPoolExecutor(1))
  var autoRebalanceFuture : Option[ScheduledFuture[_]] = None

  // Metrics
  val shortName = config.workUnitShortName
  val listGauge = metrics.gauge[String]("my_" + shortName) { myWorkUnits.mkString(", ") }
  val countGauge = metrics.gauge[Int]("my_" + shortName + "_count") { myWorkUnits.size }
  val connStateGauge = metrics.gauge[String]("zk_connection_state") { connected.get().toString }
  val nodeStateGauge = metrics.gauge[String]("node_state") { getState().toString }

  val state = new AtomicReference[NodeState.Value](NodeState.Fresh)
  def getState() : NodeState.Value = state.get()

  var zk : ZooKeeperClient = null

  /**
   * Joins the cluster, claims work, and begins operation.
   */
  def join() : String = {
    join(None)
  }

  /**
   * Joins the cluster using a custom zk client, claims work, and begins operation.
   */
  def join(injectedClient: Option[ZooKeeperClient]) : String = {
    state.get() match {
      case NodeState.Fresh    => connect(injectedClient)
      case NodeState.Shutdown => connect(injectedClient)
      case NodeState.Draining => log.warn("'join' called while draining; ignoring.")
      case NodeState.Started  => log.warn("'join' called after started; ignoring.")
    }

    state.get().toString
  }

  /**
   * registers a shutdown hook which causes cleanup of ephemeral state in zookeeper
   * when the JVM exits normally (via Ctrl+C or SIGTERM for example)
   *
   * this alerts other applications which have discovered this instance that it is
   * down so they may avoid remitting requests. otherwise this will not happen until
   * the default zookeeper timeout of 10s during which requests will fail until
   * the application is up and accepting requests again
   */
  def addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(
      new Thread() {
        override def run() {
          log.info("Cleaning up ephemeral ZooKeeper state")
          deleteFromZk()
        }
      }
    )
  }

  val connectionWatcher = new Watcher {
    def process(event: WatchedEvent) {
      event.getState match {
        case KeeperState.SyncConnected => {
          log.info("ZooKeeper session established.")
          connected.set(true)
          try {
            if (state.get() != NodeState.Shutdown)
              onConnect()
            else
              log.info("This node is shut down. ZK connection re-established, but not relaunching.")
          } catch {
            case e:Exception =>
              log.error(e, "Exception during zookeeper connection established callback")
          }
        }
        case KeeperState.Expired =>
          log.info("ZooKeeper session expired.")
          connected.set(false)
          forceShutdown()
          awaitReconnect()
        case KeeperState.Disconnected =>
          log.info("ZooKeeper session disconnected. Awaiting reconnect...")
          connected.set(false)
          awaitReconnect()
        case x: Any =>
          log.info("ZooKeeper session interrupted. Shutting down due to %s", x)
          connected.set(false)
          awaitReconnect()
      }
    }

    def awaitReconnect() {
      while (true) {
        try {
          log.info("Awaiting reconnection to ZooKeeper...")
          zk.get(Amount.of(1L, Time.SECONDS))
          return
        } catch {
          case e: TimeoutException => log.warn("Timed out reconnecting to ZooKeeper.")
          case e: Exception => log.error("Error reconnecting to ZooKeeper", e)
        }
      }

    }

  }

  /**
   * Directs the ZooKeeperClient to connect to the ZooKeeper ensemble and wait for
   * the connection to be established before continuing.
   */
  def connect(injectedClient: Option[ZooKeeperClient] = None) {
    if (!initialized.get) {
      val hosts = config.hosts.split(",").map { server =>
        val host = server.split(":")(0)
        val port = Integer.parseInt(server.split(":")(1))
        new InetSocketAddress(host, port)
      }.toList

      log.info("Connecting to hosts: %s", hosts.toString)
      zk = injectedClient.getOrElse(
        new ZooKeeperClient(Amount.of(config.zkTimeout, Time.MILLISECONDS), hosts))
      log.info("Registering connection watcher.")
      zk.register(connectionWatcher)
    }

    zk.get()
  }

  /**
   * Drains all work claimed by this node over the time period provided in the config
   * (default: 60 seconds), prevents it from claiming new work, and exits the cluster.
   */
  def shutdown() {
    if (state.get() == NodeState.Shutdown) return
    balancingPolicy.shutdown()
    if (autoRebalanceFuture.isDefined) autoRebalanceFuture.get.cancel(true)
    log.info("Shutdown initiated; beginning drain...")
    setState(NodeState.Draining)
    balancingPolicy.drainToCount(0, true)
  }

  def forceShutdown() {
    balancingPolicy.shutdown()
    if (autoRebalanceFuture.isDefined) autoRebalanceFuture.get.cancel(true)
    log.warn("Forcible shutdown initiated due to connection loss...")
    myWorkUnits.map(w => shutdownWork(w, true, false))
    myWorkUnits.clear()
    listener.onLeave()
  }

  /**
   * Finalizes the shutdown sequence. Called once the drain operation completes.
   */
  def completeShutdown() {
    setState(NodeState.Shutdown)
    myWorkUnits.map(w => shutdownWork(w))
    myWorkUnits.clear()
    deleteFromZk()
    try {
      zk.close()
    } catch {
      case e: Exception => log.warn(e, "Zookeeper reported exception on shutdown.")
    }
    listener.onLeave()
  }

  /**
   * remove this worker's ephemeral node from zk
   */
  def deleteFromZk() {
    ZKUtils.delete(zk, "/" + name + "/nodes/" + myNodeID)
  }

  /**
   * Primary callback which is triggered upon successful Zookeeper connection.
   */
  def onConnect() {
    if (state.get() != NodeState.Fresh) {
      if (previousZKSessionStillActive()) {
        log.info("ZooKeeper session re-established before timeout.")
        return
      } else {
        log.warn("Rejoined after session timeout. Forcing shutdown and clean startup.")
        ensureCleanStartup()
      }
    }

    log.info("Connected to Zookeeper (ID: %s).", myNodeID)
    ZKUtils.ensureOrdasityPaths(zk, name, config.workUnitName, config.workUnitShortName)

    joinCluster()

    listener.onJoin(zk)

    if (watchesRegistered.compareAndSet(false, true))
      registerWatchers()
    initialized.set(true)
    
    setState(NodeState.Started)
    claimWork()
    verifyIntegrity()

    balancingPolicy.onConnect()


    if (config.enableAutoRebalance)
      scheduleRebalancing()
  }

  /**
   * In the event that the node has been evicted and is reconnecting, this method
   * clears out all existing state before relaunching to ensure a clean launch.
   */
  def ensureCleanStartup() {
    forceShutdown()
    val oldPool = pool.getAndSet(new ScheduledThreadPoolExecutor(1))
    oldPool.shutdownNow()
    myWorkUnits.map(w => shutdownWork(w))
    myWorkUnits.clear()
    claimedForHandoff.clear()
    workUnitsPeggedToMe.clear()
    state.set(NodeState.Fresh)
  }

  /**
   * Schedules auto-rebalancing if auto-rebalancing is enabled. The task is
   * scheduled to run every 60 seconds by default, or according to the config.
   */
  def scheduleRebalancing() {
    val interval = config.autoRebalanceInterval
    val runRebalance = new Runnable {
      def run() {
        try {
          rebalance()
        } catch {
          case e: Exception => log.error(e, "Error running auto-rebalance.")
        }
      }
    }

    autoRebalanceFuture = Some(
      pool.get.scheduleAtFixedRate(runRebalance, interval, interval, TimeUnit.SECONDS))
  }


  /**
   * Registers this node with Zookeeper on startup, retrying until it succeeds.
   * This retry logic is important in that a node which restarts before Zookeeper
   * detects the previous disconnect could prohibit the node from properly launching.
   */
  def joinCluster() {
    while (true) {
      val myInfo = new NodeInfo(NodeState.Fresh.toString, zk.get().getSessionId)
      if (ZKUtils.createEphemeral(zk, "/" + name + "/nodes/" + myNodeID, generate(myInfo))) {
        return
      } else {
        log.warn("Unable to register with Zookeeper on launch. " +
          "Is %s already running on this host? Retrying in 1 second...", name)
        Thread.sleep(1000)
      }
    }
  }

  /**
   * Registers each of the watchers that we're interested in in Zookeeper, and callbacks.
   * This includes watchers for changes to cluster topology (/nodes), work units
   * (/work-units), and claimed work (/<service-name>/claimed-work). We also register
   * watchers for calls to "/meta/rebalance", and if smart balancing is enabled, we'll
   * watch "<service-name>/meta/workload" for changes to the cluster's workload.
   */
  def registerWatchers() {

    val nodesChangedListener = new ClusterNodesChangedListener(this)
    val verifyIntegrityListener = new VerifyIntegrityListener(this, config)
    val handoffResultsListener = new HandoffResultsListener(this, config)
    val stringDeser = new StringDeserializer()

    nodes = ZKMap.create(zk, "/%s/nodes".format(name),
      new NodeInfoDeserializer(), nodesChangedListener)

    allWorkUnits = ZKMap.create(zk, "/%s".format(config.workUnitName),
      stringDeser, verifyIntegrityListener)

    workUnitMap = ZKMap.create(zk, "/%s/claimed-%s".format(name, config.workUnitShortName),
      stringDeser, verifyIntegrityListener)

    // Watch handoff requests and results.
    if (config.useSoftHandoff) {
      handoffRequests = ZKMap.create(zk, "/%s/handoff-requests".format(name),
        stringDeser, verifyIntegrityListener)

      handoffResults = ZKMap.create(zk, "/%s/handoff-result".format(name),
        stringDeser, handoffResultsListener)
    } else {
      handoffRequests = new HashMap[String, String]
      handoffResults = new HashMap[String, String]
    }

    // If smart balancing is enabled, watch for changes to the cluster's workload.
    if (config.useSmartBalancing)
      loadMap = ZKMap.create[Double](zk, "/%s/meta/workload".format(name), new DoubleDeserializer)
  }


  /**
   * Triggers a work-claiming cycle. If smart balancing is enabled, claim work based
   * on node and cluster load. If simple balancing is in effect, claim by count.
   */
  def claimWork() {
    if (state.get != NodeState.Started || !connected.get) return
    balancingPolicy.claimWork()
  }


  /**
    * Requests that another node take over for a work unit by creating a ZNode
    * at handoff-requests. This will trigger a claim cycle and adoption.
   */
  def requestHandoff(workUnit: String) {
    log.info("Requesting handoff for %s.", workUnit)
    ZKUtils.createEphemeral(zk, "/" + name + "/handoff-requests/" + workUnit)
  }


  /**
   * Verifies that all nodes are hooked up properly. Shuts down any work units
   * which have been removed from the cluster or have been assigned to another node.
   */
  def verifyIntegrity() {
    val noLongerActive = myWorkUnits -- allWorkUnits.keys.toSet
    for (workUnit <- noLongerActive)
      shutdownWork(workUnit)

    // Check the status of pegged work units to ensure that this node is not serving
    // a work unit that is pegged to another node in the cluster.
    myWorkUnits.map { workUnit =>
      val claimPath = "/%s/claimed-%s/%s".format(name, config.workUnitShortName, workUnit)
      if (!balancingPolicy.isFairGame(workUnit) && !balancingPolicy.isPeggedToMe(workUnit)) {
        log.info("Discovered I'm serving a work unit that's now " +
          "pegged to someone else. Shutting down %s", workUnit)
        shutdownWork(workUnit)

      } else if (workUnitMap.contains(workUnit) && !workUnitMap.get(workUnit).equals(myNodeID) &&
          !claimedForHandoff.contains(workUnit) && !znodeIsMe(claimPath)) {
        log.info("Discovered I'm serving a work unit that's now " +
          "claimed by %s according to ZooKeeper. Shutting down %s", workUnitMap.get(workUnit), workUnit)
        shutdownWork(workUnit, true, false)
      }
    }
  }


  /**
   * Starts up a work unit that this node has claimed.
   * If "smart rebalancing" is enabled, hand the listener a meter to mark load.
   * Otherwise, just call "startWork" on the listener and let the client have at it.
   * TODO: Refactor to remove check and cast.
   */
  def startWork(workUnit: String, meter: Option[Meter] = None) {
    log.info("Successfully claimed %s: %s. Starting...", config.workUnitName, workUnit)
    val added = myWorkUnits.add(workUnit)

    if (added) {
      if (balancingPolicy.isInstanceOf[MeteredBalancingPolicy]) {
        val mbp = balancingPolicy.asInstanceOf[MeteredBalancingPolicy]
        val meter = mbp.persistentMeterCache.getOrElseUpdate(
          workUnit, metrics.meter(workUnit, "processing"))
        mbp.meters.put(workUnit, meter)
        listener.asInstanceOf[SmartListener].startWork(workUnit, meter)
      } else {
        listener.asInstanceOf[ClusterListener].startWork(workUnit)
      }
    } else {
      log.warn("Detected that %s is already a member of my work units; not starting twice!", workUnit)
    }
  }


  /**
   * Shuts down a work unit by removing the claim in ZK and calling the listener.
   */
  def shutdownWork(workUnit: String, doLog: Boolean = true, deleteZNode: Boolean = true) {
    if (doLog) log.info("Shutting down %s: %s...", config.workUnitName, workUnit)
    myWorkUnits.remove(workUnit)
    val path = "/%s/claimed-%s/%s".format(name, config.workUnitShortName, workUnit)
    if (deleteZNode) ZKUtils.delete(zk, path)
    balancingPolicy.onShutdownWork(workUnit)
    listener.shutdownWork(workUnit)
  }


  /**
   * Initiates a cluster rebalance. If smart balancing is enabled, the target load
   * is set to (total cluster load / node count), where "load" is determined by the
   * sum of all work unit meters in the cluster. If smart balancing is disabled,
   * the target load is set to (# of work items / node count).
   */
  def rebalance() {
    if (state.get() == NodeState.Fresh) return
    balancingPolicy.rebalance()
  }


  /**
   * Given a path, determines whether or not the value of a ZNode is my node ID.
  */
  def znodeIsMe(path: String) : Boolean = {
    val value = ZKUtils.get(zk, path)
    (value != null && value == myNodeID)
  }

  /**
   * Sets the state of the current Ordasity node and notifies others via ZooKeeper.
  */
  def setState(to: NodeState.Value) {
    val myInfo = new NodeInfo(to.toString, zk.get().getSessionId)
    ZKUtils.set(zk, "/" + name + "/nodes/" + myNodeID, generate(myInfo))
    state.set(to)
  }


  /**
   * Determines if another ZooKeeper session is currently active for the current node
   * by comparing the ZooKeeper session ID of the connection stored in NodeState.
  */
  def previousZKSessionStillActive() : Boolean = {
    try {
      val data = zk.get().getData("/%s/nodes/%s".format(name, myNodeID), false, null)
      val nodeInfo = new NodeInfoDeserializer().apply(data)
      nodeInfo.connectionID == zk.get().getSessionId
    } catch {
      case e: NoNodeException =>
        false
      case e: Exception =>
        log.error(e, "Encountered unexpected error in checking ZK session status.")
        false
    }
  }


  def getOrElse(map: Map[String, String], key: String, orElse: String) : String = {
    val result = map.get(key)
    if (result == null) orElse
    else result
  }


  def getOrElse(map: Map[String, Double], key: String, orElse: Double) : Double = {
    if (map.containsKey(key)) map.get(key) else orElse
  }

  def isMe(other: String) : Boolean = {
    myNodeID.equals(other)
  }

}
