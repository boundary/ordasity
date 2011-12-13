//
// Copyright 2011, Boundary
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
import com.twitter.zookeeper.ZooKeeperClient
import com.yammer.metrics.{Meter, Instrumented}

import java.nio.charset.Charset
import overlock.atomicmap.AtomicMap
import scala.collection.JavaConversions._
import org.cliffc.high_scale_lib.NonBlockingHashSet
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{TimeUnit, ScheduledFuture, ScheduledThreadPoolExecutor}
import java.util.{HashSet, LinkedList, TimerTask, Set => JSet, Collection => JCollection}

import javax.management.ObjectName
import java.lang.management.ManagementFactory
import org.apache.zookeeper.KeeperException.NoNodeException

trait ClusterMBean {
  def join() : String
  def shutdown()
}

object NodeState extends Enumeration {
  type NodeState = Value
  val Fresh, Started, Draining, Shutdown = Value
}

case class NodeInfo(state: String, connectionID: Long)

class Cluster(name: String, listener: Listener, config: ClusterConfig) extends ClusterMBean with Logging with Instrumented {
  val myNodeID = config.nodeId

  ManagementFactory.getPlatformMBeanServer.registerMBean(this, new ObjectName(name + ":" + "name=Cluster"))

  // Cluster, node, and work unit state
  private val nodes = AtomicMap.atomicNBHM[String, NodeInfo]
  private val meters = AtomicMap.atomicNBHM[String, Meter]
  private val persistentMeterCache = AtomicMap.atomicNBHM[String, Meter]
  private val myWorkUnits = new NonBlockingHashSet[String]
  private val allWorkUnits = AtomicMap.atomicNBHM[String, String]
  private val workUnitMap = AtomicMap.atomicNBHM[String, String]
  private val handoffRequests = new HashSet[String]
  private val handoffResults = AtomicMap.atomicNBHM[String, String]
  private val claimedForHandoff = new NonBlockingHashSet[String]
  private val loadMap = AtomicMap.atomicNBHM[String, Double]
  private val workUnitsPeggedToMe = new NonBlockingHashSet[String]

  // Scheduled executions
  private val pool = new ScheduledThreadPoolExecutor(1)
  private var loadFuture : Option[ScheduledFuture[_]] = None
  private var autoRebalanceFuture : Option[ScheduledFuture[_]] = None

  // Metrics
  val listGauge = metrics.gauge[String]("my_" + config.workUnitShortName) { myWorkUnits.mkString(", ") }
  val countGauge = metrics.gauge[Int]("my_" + config.workUnitShortName + "_count") { myWorkUnits.size }
  val loadGauge = metrics.gauge[Double]("my_load") { myLoad() }

  private val state = new AtomicReference[NodeState.Value](NodeState.Fresh)

  var zk : ZooKeeperClient = null

  /**
   * Joins the cluster, claims work, and begins operation.
   */
  def join() : String = {
    state.get() match {
      case NodeState.Fresh    => zk = new ZooKeeperClient(config.hosts, config.zkTimeout, "/", onConnect(_))
      case NodeState.Shutdown => zk = new ZooKeeperClient(config.hosts, config.zkTimeout, "/", onConnect(_))
      case NodeState.Draining => log.warn("'join' called while draining; ignoring.")
      case NodeState.Started  => log.warn("'join' called after started; ignoring.")
    }

    state.get().toString
  }

  /**
   * Drains all work claimed by this node over the time period provided in the config
   * (default: 60 seconds), prevents it from claiming new work, and exits the cluster.
   */
  def shutdown() {
    if (state.get() == NodeState.Shutdown) return

    if (loadFuture.isDefined) loadFuture.get.cancel(true)
    if (autoRebalanceFuture.isDefined) autoRebalanceFuture.get.cancel(true)
    log.info("Shutdown initiated; beginning drain...")
    setState(NodeState.Draining)
    drainToCount(0, true)
  }

  def forceShutdown() {
    if (loadFuture.isDefined) loadFuture.get.cancel(true)
    if (autoRebalanceFuture.isDefined) autoRebalanceFuture.get.cancel(true)
    log.warn("Forcible shutdown initiated due to connection loss...")
    myWorkUnits.map(w => shutdownWork(w, true, false))
    myWorkUnits.clear()
    listener.onLeave()
  }

  /**
   * Finalizes the shutdown sequence. Called once the drain operation completes.
   */
  private def completeShutdown() {
    setState(NodeState.Shutdown)
    try {
      zk.close()
    } catch {
      case e: Exception => log.warn(e, "Zookeeper reported exception on shutdown.")
    }
    listener.onLeave()
    state.set(NodeState.Fresh)
  }

  /**
   * Primary callback which is triggered upon successful Zookeeper connection.
   */
  private def onConnect(client: ZooKeeperClient) {
    zk = client

    if (state.get() != NodeState.Fresh) {
      if (previousZKSessionStillActive()) {
        log.info("Zookeeper session re-established before timeout. No need to exit and rejoin cluster.")
        return
      } else {
        log.warn("Rejoined after Zookeeper session timeout. Initiating forced shutdown and clean startup.")
        ensureCleanStartup()
      }
    }

    log.info("Connected to Zookeeper (ID: %s).", myNodeID)
    zk.createPath(name + "/nodes")
    zk.createPath(config.workUnitName)
    zk.createPath(name + "/meta/rebalance")
    zk.createPath(name + "/meta/workload")
    zk.createPath(name + "/claimed-" + config.workUnitShortName)
    zk.createPath(name + "/handoff-requests")
    zk.createPath(name + "/handoff-result")
    joinCluster()

    listener.onJoin(zk)

    registerWatchers()

    setState(NodeState.Started)
    claimWork()
    verifyIntegrity()

    if (config.useSmartBalancing && listener.isInstanceOf[SmartListener])
      scheduleLoadTicks()

    if (config.enableAutoRebalance)
      scheduleRebalancing()
  }

  def ensureCleanStartup() {
    forceShutdown()
    nodes.clear()
    myWorkUnits.clear()
    allWorkUnits.clear()
    workUnitMap.clear()
    handoffRequests.clear()
    handoffResults.clear()
    loadMap.clear()
    claimedForHandoff.clear()
    workUnitsPeggedToMe.clear()
    state.set(NodeState.Fresh)
  }

  /**
   * Schedules auto-rebalancing if auto-rebalancing is enabled. The task is
   * scheduled to run every 60 seconds by default, or according to the config.
   */
  private def scheduleRebalancing() {
    val runRebalance = new Runnable {
      def run() = rebalance()
    }

    autoRebalanceFuture = Some(pool.scheduleAtFixedRate(runRebalance, config.autoRebalanceInterval,
      config.autoRebalanceInterval, TimeUnit.SECONDS))
  }

  /**
   * Once a minute, pass off information about the amount of load generated per
   * work unit off to Zookeeper for use in the claiming and rebalancing process.
   */
  private def scheduleLoadTicks() {
    val sendLoadToZookeeper = new Runnable {
      def run() {
        meters.foreach { case(workUnit, meter) =>
          ZKUtils.setOrCreate(zk, name + "/meta/workload/" + workUnit, meter.oneMinuteRate.toString)
        }
        val myInfo = new NodeInfo(state.get.toString, zk.getHandle().getSessionId)
        ZKUtils.setOrCreate(zk, name + "/nodes/" + myNodeID, generate(myInfo))

        if (config.useSmartBalancing)
          log.info("My load: %s", myLoad())
        else
          log.info("My load: %s", myWorkUnits.size)
      }
    }

    if (config.useSmartBalancing && listener.isInstanceOf[SmartListener])
      loadFuture = Some(pool.scheduleAtFixedRate(sendLoadToZookeeper, 0, 1, TimeUnit.MINUTES))
  }

  /**
   * Registers this node with Zookeeper on startup, retrying until it succeeds.
   * This retry logic is important in that a node which restarts before Zookeeper
   * detects the previous disconnect could prohibit the node from properly launching.
   */
  private def joinCluster() {
    while (true) {
      val myInfo = new NodeInfo(NodeState.Fresh.toString, zk.getHandle().getSessionId)
      if (ZKUtils.createEphemeral(zk, name + "/nodes/" + myNodeID, generate(myInfo))) {
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
  private def registerWatchers() {
    zk.watchChildrenWithData[NodeInfo](name + "/nodes", nodes, bytesToNodeInfo(_), { data: String =>
      log.info("Nodes: %s".format(nodes.map(n => n._1).mkString(", ")))
      claimWork()
      verifyIntegrity()
    })

    zk.watchChildrenWithData[String](config.workUnitName,
        allWorkUnits, bytesToString(_), { data: String =>
      log.debug(config.workUnitName.capitalize + " IDs: %s".format(allWorkUnits.keys.mkString(", ")))
      claimWork()
      verifyIntegrity()
    })

    zk.watchChildrenWithData[String](name + "/claimed-" + config.workUnitShortName,
        workUnitMap, bytesToString(_), { data: String =>
      log.debug(config.workUnitName.capitalize + " / Node Mapping changed: %s", workUnitMap)
      claimWork()
      verifyIntegrity()
    })

    if (config.useSoftHandoff) {
      // Watch handoff requests.
      zk.watchChildren(name + "/handoff-requests", { (newWorkUnits: Seq[String]) =>
        refreshSet(handoffRequests, newWorkUnits)
        log.debug("Handoff requests changed: %s".format(handoffRequests.mkString(", ")))
        verifyIntegrity()
        claimWork()
      })

      // Watch handoff results.
      zk.watchChildrenWithData[String](name + "/handoff-result",
        handoffResults, bytesToString(_), { workUnit: String =>

        // If I am the node which accepted this handoff, finish the job.
        val destinationNode = handoffResults.get(workUnit).getOrElse("")
        if (myWorkUnits.contains(workUnit) && myNodeID.equals(destinationNode))
          finishHandoff(workUnit)

        // If I'm the node that requested to hand off this work unit to another node, shut it down after <config> seconds.
        else if (myWorkUnits.contains(workUnit) && !destinationNode.equals("") && !myNodeID.equals(destinationNode)) {
          log.info("Handoff of %s to %s completed. Shutting down %s in %s seconds.",
            workUnit, handoffResults.get(workUnit).getOrElse("(None)"), workUnit, config.handoffShutdownDelay)
          ZKUtils.delete(zk, name + "/handoff-requests/" + workUnit)

          val runnable = new Runnable {
            def run() {
              log.info("Shutting down %s following handoff to %s.",
                workUnit, handoffResults.get(workUnit).getOrElse("(None)"))
              shutdownWork(workUnit, false, true)
              if (myWorkUnits.size() == 0 && state.get() == NodeState.Draining)
                shutdown()
            }
          };

          pool.schedule(runnable, config.handoffShutdownDelay, TimeUnit.SECONDS).asInstanceOf[Unit]
        }
      })
    }

    // Watch for rebalance requests.
    zk.watchNode(name + "/meta/rebalance", rebalance(_))

    // If smart balancing is enabled, watch for changes to the cluster's workload.
    if (config.useSmartBalancing && listener.isInstanceOf[SmartListener])
      zk.watchChildrenWithData[Double](name + "/meta/workload", loadMap, bytesToDouble(_))
  }

  /**
   * Triggers a work-claiming cycle. If smart balancing is enabled, claim work based
   * on node and cluster load. If simple balancing is in effect, claim by count.
   */
  private def claimWork() {
    if (state.get != NodeState.Started) return

    if (config.useSmartBalancing && listener.isInstanceOf[SmartListener])
      claimByLoad()
    else
      claimByCount()
  }

  /**
   * Begins by claimng all work units that are pegged to this node.
   * Then, continues to c state.get  ==laim work from the available pool until we've claimed
   * equal to or slightly more than the total desired load.
   */
  private def claimByLoad() {
    allWorkUnits.synchronized {

      val peggedCheck = new LinkedList[String](allWorkUnits.keys.toSet -- workUnitMap.keys.toSet --
        myWorkUnits ++ handoffRequests -- handoffResults.keys)
      for (workUnit <- peggedCheck)
        if (isPeggedToMe(workUnit))
          claimWorkPeggedToMe(workUnit)

      val unclaimed = new LinkedList[String](peggedCheck -- myWorkUnits)

      while (myLoad() <= evenDistribution && !unclaimed.isEmpty) {
        val workUnit = unclaimed.poll()

        if (config.useSoftHandoff && handoffRequests.contains(workUnit)
            && isFairGame(workUnit) && attemptToClaim(workUnit, true))
          log.info("Accepted handoff for %s.", workUnit)
        else if (isFairGame(workUnit))
          attemptToClaim(workUnit)
      }
    }
  }

  /**
    * Claims work in Zookeeper. This method will attempt to divide work about the cluster
    * by claiming up to ((<x> Work Unit Count / <y> Nodes) + 1) work units. While
    * this doesn't necessarily represent an even load distribution based on work unit load,
    * it should result in a relatively even "work unit count" per node. This randomly-distributed
    * amount is in addition to any work units which are pegged to this node.
   */
  private def claimByCount() {
    var claimed = myWorkUnits.size
    val nodeCount = activeNodeSize()

    allWorkUnits.synchronized {
      val maxToClaim = {
        if (allWorkUnits.size <= 1) allWorkUnits.size
        else (allWorkUnits.size / nodeCount.toDouble).ceil
      }

      log.debug("%s Nodes: %s. %s: %s.", name, nodeCount, config.workUnitName.capitalize, allWorkUnits.size)
      log.debug("Claiming %s pegged to me, and up to %s more.", config.workUnitName, maxToClaim)

      val unclaimed = allWorkUnits.keys.toSet -- workUnitMap.keys.toSet ++ handoffRequests -- handoffResults.keys
      log.debug("Handoff requests: %s, Handoff Results: %s, Unclaimed: %s",
        handoffRequests.mkString(", "), handoffResults.mkString(", "), unclaimed.mkString(", "))

      for (workUnit <- unclaimed) {
        if ((isFairGame(workUnit) && claimed < maxToClaim) || isPeggedToMe(workUnit)) {

          if (config.useSoftHandoff && handoffRequests.contains(workUnit) && attemptToClaim(workUnit, true)) {
            log.info("Accepted handoff of %s.", workUnit)
            claimed += 1
          } else if (!handoffRequests.contains(workUnit) && attemptToClaim(workUnit)) {
            claimed += 1
          }
        }
      }
    }
  }

  def finishHandoff(workUnit: String) {
    log.info("Handoff of %s to me acknowledged. Deleting claim ZNode for %s and waiting for " +
      "%s to shutdown work.", workUnit, workUnit, workUnitMap.get(workUnit).getOrElse("(None)"))

    val claimPostHandoffTask = new TimerTask {
      def run() {
        val path = name + "/claimed-" + config.workUnitShortName + "/" + workUnit
        if (ZKUtils.createEphemeral(zk, path, myNodeID) || znodeIsMe(path)) {
          ZKUtils.delete(zk, name + "/handoff-result/" + workUnit)
          claimedForHandoff.remove(workUnit)
          log.warn("Handoff of %s to me complete. Peer has shut down work.", workUnit)
        } else {
          log.warn("Waiting to establish final ownership of %s following handoff...", workUnit)
          pool.schedule(this, 2, TimeUnit.SECONDS)
        }
      }
    }

    pool.schedule(claimPostHandoffTask, config.handoffShutdownDelay, TimeUnit.SECONDS)
  }

  def attemptToClaim(workUnit: String, claimForHandoff: Boolean = false) : Boolean = {
    val path = {
      if (claimForHandoff) name + "/handoff-result/" + workUnit
      else name + "/claimed-" + config.workUnitShortName + "/" + workUnit
    }

    val created = ZKUtils.createEphemeral(zk, path, myNodeID)

    if (created) {
      if (claimForHandoff) claimedForHandoff.add(workUnit)
      startWork(workUnit)
      true
    } else if (isPeggedToMe(workUnit)) {
      claimWorkPeggedToMe(workUnit)
      true
    } else {
      false
    }
  }

  /**
    * Requests that another node take over for a work unit by creating a ZNode
    * at handoff-requests. This will trigger a claim cycle and adoption.
   */
  def requestHandoff(workUnit: String) {
    log.info("Requesting handoff for %s.", workUnit)
    ZKUtils.createEphemeral(zk, name + "/handoff-requests/" + workUnit)
  }


  /**
    * Determines whether or not a given work unit is designated "claimable" by this node.
    * If the ZNode for this work unit is empty, or contains JSON mapping this node to that
    * work unit, it's considered "claimable."
   */
  private def isFairGame(workUnit: String) : Boolean = {
    val workUnitData = allWorkUnits.get(workUnit)
    if (workUnitData.isEmpty || workUnitData.get.equals(""))
      return true

    val mapping = parse[Map[String, String]](workUnitData.get)
    val pegged = mapping.get(name)
    if (pegged.isDefined) log.debug("Pegged status for %s: %s.", workUnit, pegged.get)
    (pegged.isEmpty || (pegged.isDefined && pegged.get.equals(myNodeID)) ||
      (pegged.isDefined && pegged.get.equals("")))
  }

  /**
   * Determines whether or not a given work unit is pegged to this instance.
   */
  private def isPeggedToMe(workUnitId: String) : Boolean = {
    val zkWorkData = allWorkUnits.get(workUnitId).get
    if (zkWorkData.isEmpty) {
      workUnitsPeggedToMe.remove(workUnitId)
      return false
    }

    val mapping = parse[Map[String, String]](zkWorkData)
    val pegged = mapping.get(name)
    val isPegged = (pegged.isDefined && (pegged.get.equals(myNodeID)))

    if (isPegged) workUnitsPeggedToMe.add(workUnitId)
    else workUnitsPeggedToMe.remove(workUnitId)

    isPegged
  }

  /**
   * Verifies that all nodes are hooked up properly. Shuts down any work units
   * which have been removed from the cluster or have been assigned to another node.
   */
  private def verifyIntegrity() {
    val noLongerActive = myWorkUnits -- allWorkUnits.keys.toSet
    for (workUnit <- noLongerActive)
      shutdownWork(workUnit)

    // Check the status of pegged work units to ensure that this node is not serving
    // a work unit that is pegged to another node in the cluster.
    myWorkUnits.map { workUnit =>
      if (!isFairGame(workUnit) && !isPeggedToMe(workUnit)) {
        log.info("Discovered I'm serving a work unit that's now " +
          "pegged to someone else. Shutting down %s", workUnit)
        shutdownWork(workUnit)

      } else if (workUnitMap.contains(workUnit) && !workUnitMap.get(workUnit).get.equals(myNodeID) &&
          !claimedForHandoff.contains(workUnit)) {
        log.info("Discovered I'm serving a work unit that's now " +
          "served by %s. Shutting down %s", workUnitMap.get(workUnit).get, workUnit)
        shutdownWork(workUnit, true, false)
      }
    }
  }

  /**
   * Claims a work unit pegged to this node, waiting for the ZNode to become available
   * (i.e., deleted by the node which previously owned it).
   */
  private def claimWorkPeggedToMe(workUnit: String) {
    while (true) {
      if (ZKUtils.createEphemeral(zk,
          name + "/claimed-" + config.workUnitShortName + "/" + workUnit, myNodeID)) {
        startWork(workUnit)
        return
      } else {
        log.warn("Attempting to establish ownership of %s. Retrying in one second...", workUnit)
        Thread.sleep(1000)
      }
    }
  }

  /**
   * Starts up a work unit that this node has claimed.
   * If "smart rebalancing" is enabled, hand the listener a meter to mark load.
   * Otherwise, just call "startWork" on the listener and let the client have at it.
   */
  private def startWork(workUnit: String) {
    log.info("Successfully claimed %s: %s. Starting...", config.workUnitName, workUnit)
    myWorkUnits.add(workUnit)

    if (listener.isInstanceOf[SmartListener]) {
      val meter = persistentMeterCache.getOrElseUpdate(workUnit, metrics.meter(workUnit, "processing"))
      meters.put(workUnit, meter)
      listener.asInstanceOf[SmartListener].startWork(workUnit, meter)
    } else {
      listener.asInstanceOf[ClusterListener].startWork(workUnit)
    }
  }

  /**
   * Shuts down a work unit by removing the claim in ZK and calling the listener.
   */
  private def shutdownWork(workUnit: String, doLog: Boolean = true, deleteZNode: Boolean = true) {
    if (doLog) log.info("Shutting down %s: %s...", config.workUnitName, workUnit)
    myWorkUnits.remove(workUnit)
    if (deleteZNode) ZKUtils.delete(zk, name + "/claimed-" + config.workUnitShortName + "/" + workUnit)
    meters.remove(workUnit)
    listener.shutdownWork(workUnit)
  }

  /**
   * Drains excess load on this node down to a fraction distributed across the cluster.
   * The target load is set to (clusterLoad / # nodes).
   */
  private def drainToLoad(targetLoad: Long, time: Int = config.drainTime, useHandoff: Boolean = config.useSoftHandoff) {
    var currentLoad = myLoad()
    val drainList = new LinkedList[String]
    val eligibleToDrop = new LinkedList[String](myWorkUnits -- workUnitsPeggedToMe)

    while (currentLoad > targetLoad && !eligibleToDrop.isEmpty) {
      val workUnit = eligibleToDrop.poll()
      val workUnitLoad : Double = loadMap.get(workUnit).getOrElse(0)

      if (workUnitLoad > 0 && currentLoad - workUnitLoad > targetLoad) {
        drainList.add(workUnit)
        currentLoad -= workUnitLoad
      }
    }
    val drainInterval = ((config.drainTime.toDouble / drainList.size) * 1000).intValue()

    val drainTask = new TimerTask {

      def run() {
        if (drainList.isEmpty || myLoad <= evenDistribution)
          return
        else if (useHandoff)
          requestHandoff(drainList.poll)
        else
          shutdownWork(drainList.poll)

        pool.schedule(this, drainInterval, TimeUnit.MILLISECONDS)
      }
    }

    if (!drainList.isEmpty) {
      log.info("Releasing work units over %s seconds. Current load: %s. Target: %s. " +
        "Releasing: %s", time, currentLoad, targetLoad, drainList.mkString(", "))
      pool.schedule(drainTask, 0, TimeUnit.SECONDS)
    }
  }

  /**
   * Drains this node's share of the cluster workload down to a specific number
   * of work units over a period of time specified in the configuration with soft handoff if enabled..
   */
  def drainToCount(targetCount: Int, doShutdown: Boolean = false, useHandoff: Boolean = config.useSoftHandoff) {
    val msg = if (useHandoff) " with handoff" else ""
    log.info("Draining %s%s. Target count: %s, Current: %s", config.workUnitName, msg, targetCount, myWorkUnits.size)
    if (targetCount >= myWorkUnits.size)
      return

    val amountToDrain = myWorkUnits.size - targetCount

    val msgPrefix = if (useHandoff) "Requesting handoff for " else "Shutting down "
    log.info("%s %s of %s %s over %s seconds",
      msgPrefix, amountToDrain, myWorkUnits.size, config.workUnitName, config.drainTime)

    // Build a list of work units to hand off.
    val toHandOff = new LinkedList[String]
    val wuList = myWorkUnits.toList
    for (i <- (0 to amountToDrain - 1))
      if (wuList.size - 1 >= i) toHandOff.add(wuList(i))

    val drainInterval = ((config.drainTime.toDouble / toHandOff.size) * 1000).intValue()

    val handoffTask = new TimerTask {
      def run() {
        if (toHandOff.isEmpty) {
          if (targetCount == 0 && doShutdown) completeShutdown()
          return
        } else {
          val workUnit = toHandOff.poll()
          if (useHandoff && !isPeggedToMe(workUnit)) requestHandoff(workUnit)
          else shutdownWork(workUnit)
        }
        pool.schedule(this, drainInterval, TimeUnit.MILLISECONDS)
      }
    }

    log.info("Releasing %s / %s work units over %s seconds: %s",
      amountToDrain, myWorkUnits.size, config.drainTime, toHandOff.mkString(", "))

    if (!myWorkUnits.isEmpty)
      pool.schedule(handoffTask, 0, TimeUnit.SECONDS)
  }


  /**
   * Initiates a cluster rebalance. If smart balancing is enabled, the target load
   * is set to (total cluster load / node count), where "load" is determined by the
   * sum of all work unit meters in the cluster. If smart balancing is disabled,
   * the target load is set to (# of work items / node count).
   */
  def rebalance(data: Option[Array[Byte]] = null) {
    if (state.get() == NodeState.Fresh) return

    if (config.useSmartBalancing && listener.isInstanceOf[SmartListener])
      smartRebalance()
    else
      simpleRebalance()
  }

  /**
   * Performs a "smart rebalance." The target load is set to (cluster load / node count),
   * where "load" is determined by the sum of all work unit meters in the cluster.
   */
  private def smartRebalance() {
    val target = evenDistribution()
    if (myLoad() > target) {
      log.info("Smart Rebalance triggered. Load: %s. Target: %s", myLoad(), target)
      drainToLoad(target.longValue)
    }
  }

  /**
   * Performs a simple rebalance. Target load is set to (# of work items / node count).
   */
  private def simpleRebalance(data: Option[Array[Byte]] = null) {
    val target = fairShare()

    if (myWorkUnits.size > target) {
      log.info("Simple Rebalance triggered. My Share: %s. Target: %s.",  myWorkUnits.size, target)
      drainToCount(target)
    }
  }

  /**
   * Determines the current load on this instance when smart rebalancing is enabled.
   * This load is determined by the sum of all of this node's meters' one minute rate.
   */
  private def myLoad() : Double = {
    var load = 0d
    log.debug(loadMap.toString)
    log.debug(myWorkUnits.toString)
    myWorkUnits.foreach(u => load += loadMap.get(u).getOrElse(0d))
    load
  }

  /**
   * When smart balancing is enabled, calculates the even distribution of load about
   * the cluster. This is determined by the total load divided by the number of alive nodes.
   */
  private def evenDistribution() : Double = {
    loadMap.values.sum / activeNodeSize().toDouble
  }

  private def fairShare() : Int = {
    (allWorkUnits.size.toDouble / activeNodeSize()).ceil.toInt
  }

  private def activeNodeSize() : Int = {
    nodes.filter(n => n._2 != null && n._2.state == NodeState.Started.toString).size
  }

  /**
   * Utility method for converting an array of bytes to a string.
   */
  private def bytesToString(bytes: Array[Byte]) : String = {
    new String(bytes, Charset.forName("UTF-8"))
  }

  /**
   * Utility method for converting an array of bytes to a NodeInfo object.
   */
  private def bytesToNodeInfo(bytes: Array[Byte]) : NodeInfo = {
    val data = new String(bytes, Charset.forName("UTF-8"))
    try {
      parse[NodeInfo](data)
    } catch {
      case e: Exception =>
        val parsedState = NodeState.valueOf(data).getOrElse(NodeState.Shutdown)
        val info = new NodeInfo(parsedState.toString, 0)
        log.warn("Saw node data in non-JSON format. Interpreting %s as: %s", data, info)
        info
    }
  }

  /**
   * Utility method for converting an array of bytes to a double.
   */
  private def bytesToDouble(bytes: Array[Byte]) : Double = {
    bytesToString(bytes).toDouble
  }

  /**
   * Utility method for swapping out the contents of a set while holding its lock.
   */
  private def refreshSet(oldSet: JSet[String], newSet: JCollection[String]) {
    oldSet.synchronized {
      oldSet.clear()
      oldSet.addAll(newSet)
    }
  }

  /**
   * Given a path, determines whether or not the value of a ZNode is my node ID.
  */
  def znodeIsMe(path: String) : Boolean = {
    val value = ZKUtils.get(zk, path)
    (value != null && value == myNodeID)
  }

  private def setState(to: NodeState.Value) {
    val myInfo = new NodeInfo(state.get.toString, zk.getHandle().getSessionId)
    ZKUtils.set(zk, name + "/nodes/" + myNodeID, generate(myInfo))
    state.set(to)
  }

  private def previousZKSessionStillActive() : Boolean = {
    try {
      val nodeInfo = bytesToNodeInfo(zk.get(name + "/nodes/" + myNodeID))
      nodeInfo.connectionID == zk.getHandle().getSessionId
    } catch {
      case e: NoNodeException =>
        false
      case e: Exception =>
        log.error(e, "Encountered unexpected error in checking ZK session status.")
        false
    }
  }

}
