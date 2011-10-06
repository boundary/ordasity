package com.boundary.cluster

import com.codahale.jerkson.Json._
import com.codahale.logula.Logging
import com.yammer.metrics.{Meter, Instrumented}
import com.twitter.zookeeper.ZooKeeperClient

import overlock.atomicmap.AtomicMap
import scala.collection.JavaConversions._
import org.cliffc.high_scale_lib.NonBlockingHashSet
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}
import java.util.concurrent.{TimeUnit, ScheduledFuture, ScheduledThreadPoolExecutor}
import java.util.{ArrayList, LinkedList, TimerTask}
import java.nio.charset.Charset

object NodeState extends Enumeration {
  type NodeState = Value
  val Fresh, Started, Draining, Shutdown = Value
}

class Cluster(name: String, listener: Listener, config: ClusterConfig) extends Logging with Instrumented {
  val myNodeID = config.nodeId

  // Cluster, node, and work unit state
  private val nodes = new ArrayList[String]()
  private val meters = AtomicMap.atomicNBHM[String, Meter]
  private val myWorkUnits = new NonBlockingHashSet[String]
  private val allWorkUnits = AtomicMap.atomicNBHM[String, String]
  private val workUnitMap = AtomicMap.atomicNBHM[String, String]
  private val loadMap = AtomicMap.atomicNBHM[String, Double]
  private val workUnitsPeggedToMe = new NonBlockingHashSet[String]

  // Scheduled executions
  private val pool = new ScheduledThreadPoolExecutor(1)
  private var loadFuture : Option[ScheduledFuture[_]] = None
  private var autoRebalanceFuture : Option[ScheduledFuture[_]] = None

  // Metrics
  val listGauge = metrics.gauge[String]("my_" + config.workUnitShortName) { myWorkUnits.mkString(", ") }
  val countGauge = metrics.gauge[Int]("my_" + config.workUnitShortName + "_count") { myWorkUnits.size }

  private val zkNodeCreated = new AtomicBoolean(false)
  private val state = new AtomicReference[NodeState.Value](NodeState.Fresh)

  var zk : ZooKeeperClient = null

  /**
   * Joins the cluster, claims work, and begins operation.
   */
  def join() : Cluster = {
    state.get() match {
      case NodeState.Fresh    => zk = new ZooKeeperClient(config.hosts, config.zkTimeout, "/", onConnect(_))
      case NodeState.Shutdown => zk = new ZooKeeperClient(config.hosts, config.zkTimeout, "/", onConnect(_))
      case NodeState.Draining => log.warn("'join' called while draining; ignoring.")
      case NodeState.Started  => log.warn("'join' called after started; ignoring.")
    }

    this
  }

  /**
   * Drains all work claimed by this node over the time period provided in the config
   * (default: 60 seconds), prevents it from claiming new work, and exits the cluster.
   */
  def shutdown() {
    log.info("Shutdown initiated; beginning drain...")

    if (loadFuture.isDefined)
      loadFuture.get.cancel(true)

    if (autoRebalanceFuture.isDefined)
      autoRebalanceFuture.get.cancel(true)

    state.set(NodeState.Draining)
    drainToCount(0, true)
  }

  /**
   * Finalizes the shutdown sequence. Called once the drain operation completes.
   */
  private def completeShutdown() {
    zk.close()
    zkNodeCreated.set(false)
    state.set(NodeState.Shutdown)
  }

  /**
   * Primary callback which is triggered upon successful Zookeeper connection.
   */
  private def onConnect(client: ZooKeeperClient) {
    zk = client

    log.info("Connected to Zookeeper (ID: %s).", myNodeID)
    zk.createPath(name + "/nodes")
    zk.createPath(config.workUnitName)
    zk.createPath(name + "/meta/rebalance")
    zk.createPath(name + "/meta/workload")
    zk.createPath(name + "/claimed-" + config.workUnitShortName)
    joinCluster()
    registerWatchers()

    state.set(NodeState.Started)
    claimWork()
    verifyIntegrity()

    if (config.useSmartBalancing && listener.isInstanceOf[SmartListener])
      scheduleLoadTicks()

    if (config.enableAutoRebalance)
      scheduleRebalancing()
  }

  /**
   * Schedules auto-rebalancing if auto-rebalancing is enabled. The task is
   * scheduled to run every 60 seconds by default, or according to the config.
   */
  private def scheduleRebalancing() {
    val runRebalance = new Runnable {
      def run() = rebalance()
    }

    autoRebalanceFuture = Some(pool.scheduleAtFixedRate(runRebalance, 0,
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
        ZKUtils.setOrCreate(zk, name + "/nodes/" + myNodeID, myLoad().toString)

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
    while (!zkNodeCreated.get) {
      if (ZKUtils.createEphemeral(zk, name + "/nodes/" + myNodeID)) {
        zkNodeCreated.set(true)
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
    zk.watchChildren(name + "/nodes", { (newNodes: Seq[String]) =>
     nodes.synchronized {
       nodes.clear()
       nodes.addAll(newNodes)
     }

      log.info("Nodes: %s".format(nodes.mkString(", ")))
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
      val unclaimedWork = allWorkUnits.keys.toSet -- workUnitMap.keys
      if (!unclaimedWork.isEmpty) claimWork()
      verifyIntegrity()
    })

    zk.watchNode(name + "/meta/rebalance", rebalance(_))

    if (config.useSmartBalancing && listener.isInstanceOf[SmartListener])
      zk.watchChildrenWithData[Double](name + "/meta/workload", loadMap, bytesToDouble(_))
  }

  /**
   * Triggers a work-claiming cycle. If smart balancing is enabled, claim work based
   * on node and cluster load. If simple balancing is in effect, claim by count.
   */
  private def claimWork() {
    if (config.useSmartBalancing && listener.isInstanceOf[SmartListener])
      claimByLoad()
    else
      claimByCount()
  }

  /**
   * Begins by claimng all work units that are pegged to this node.
   * Then, continues to claim work from the available pool until we've claimed
   * equal to or slightly more than the total desired load.
   */
  private def claimByLoad() {
    allWorkUnits.synchronized {

      val unclaimed = new LinkedList[String](allWorkUnits.keys.toSet -- workUnitMap.keys.toSet -- myWorkUnits)
      for (workUnit <- unclaimed) {
        if (isPeggedToMe(workUnit)) {
          claimWorkPeggedToMe(workUnit)
          unclaimed.remove(workUnit)
        }
      }

      while (myLoad() <= evenDistribution && !unclaimed.isEmpty) {
        val workUnit = unclaimed.poll()
        val created = ZKUtils.createEphemeral(zk,
          name + "/claimed-" + config.workUnitShortName + "/" + workUnit, myNodeID)

        if (created)
          startWork(workUnit)
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
    if (state.get != NodeState.Started) return
    var claimed = myWorkUnits.size
    val nodeCount = nodes.synchronized(nodes.size)

    allWorkUnits.synchronized {
      val maxToClaim = {
        if (allWorkUnits.size <= 1) allWorkUnits.size
        else (allWorkUnits.size / nodeCount.toDouble).ceil
      }

      log.debug("%s Nodes: %s. %s: %s.", name, nodeCount, config.workUnitName.capitalize, allWorkUnits.size)
      log.debug("Claiming %s pegged to me, and up to %s more.", config.workUnitName, maxToClaim)

      val unclaimed = allWorkUnits.keys.toSet -- workUnitMap.keys.toSet

      for (workUnit <- unclaimed) {
        if ((isFairGame(workUnit) && claimed < maxToClaim) || isPeggedToMe(workUnit)) {
          val created = ZKUtils.createEphemeral(zk,
            name + "/claimed-" + config.workUnitShortName + "/" + workUnit, myNodeID)

          if (created) {
            startWork(workUnit)
            claimed += 1
          } else {
            if (isPeggedToMe(workUnit))
              claimWorkPeggedToMe(workUnit)
          }
        }
      }
    }

    if (claimed > 0)
      log.info("Claimed %s %s.", claimed, config.workUnitName)
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
        log.warn("Error establishing ownership of %s. Retrying in one second...", workUnit)
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
    log.info("Successfully claimed work unit: %s. Starting...", workUnit)
    myWorkUnits.add(workUnit)

    if (listener.isInstanceOf[SmartListener]) {
      val meter = metrics.meter(workUnit, "processing")
      meters.put(workUnit, meter)
      listener.asInstanceOf[SmartListener].startWork(workUnit, meter)
    } else {
      listener.asInstanceOf[ClusterListener].startWork(workUnit)
    }
  }

  /**
   * Shuts down a work unit by removing the claim in ZK and calling the listener.
   */
  private def shutdownWork(workUnit: String) {
    log.info("Shutting down work unit: %s...", workUnit)
    myWorkUnits.remove(workUnit)
    ZKUtils.delete(zk, name + "/claimed-" + config.workUnitShortName + "/" + workUnit)
    meters.remove(workUnit)
    listener.shutdownWork(workUnit)
  }

  /**
   * Prevents this node from claiming new work by setting its status to "Draining,"
   * and unclaims all current work over a config-defined period of time.
   */
  def drain() {
    log.info("Initiating drain...")
    state.set(NodeState.Draining)
    drainToCount(0)
  }

  /**
   * Drains excess load on this node down to a fraction distributed across the cluster.
   * The target load is set to (clusterLoad / # nodes).
   */
  private def drainToLoad(targetLoad: Long, time: Int = config.drainTime) {
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

    val drainTask = new TimerTask {
      def run() {
        if (drainList.isEmpty || myLoad <= evenDistribution)
          cancel()
        else
          shutdownWork(drainList.poll())
      }
    }

    if (!drainList.isEmpty) {
      log.info("Releasing work units over %s seconds. Current load: %s. Target: %s. " +
        "Releasing: %s", time, currentLoad, targetLoad, drainList.mkString(", "))
      scheduleDrain(drainTask, drainList.size)
    }
  }

  /**
   * Schedules a task to unclaim <n> work units evenly over an interval of time
   * specified by the configuration.
   */
  private def scheduleDrain(task: TimerTask, numToDrain: Int) {
    pool.scheduleAtFixedRate(task, 0,
      ((config.drainTime.toDouble / numToDrain) * 1000).toInt, TimeUnit.MILLISECONDS)
  }

  /**
   * Drains this node's share of the cluster workload down to a specific number
   * of work units over a period of time specified in the configuration.
   */
  def drainToCount(targetCount: Int, shutdown: Boolean = false) {
    log.info("Draining. Target count: %s, Current: %s", targetCount, myWorkUnits.size)
    if (targetCount >= myWorkUnits.size)
      return

    log.info("Releasing %s / %s work units over %s seconds",
      myWorkUnits.size - targetCount, myWorkUnits.size, config.drainTime)

    val drainTask = new TimerTask {
      def run() {
        if (myWorkUnits.size == targetCount) {
          cancel()
          if (targetCount == 0 && shutdown)
            completeShutdown()

        } else if (targetCount == 0) {
          shutdownWork(myWorkUnits.head)

        } else if (targetCount > 0) {
          shutdownWork((myWorkUnits -- workUnitsPeggedToMe).head)
        }
      }
    }

    if (!myWorkUnits.isEmpty)
      scheduleDrain(drainTask, myWorkUnits.size() - targetCount)
  }

  /**
   * Initiates a cluster rebalance. If smart balancing is enabled, the target load
   * is set to (total cluster load / node count), where "load" is determined by the
   * sum of all work unit meters in the cluster. If smart balancing is disabled,
   * the target load is set to (# of work items / node count).
   */
  def rebalance(data: Option[Array[Byte]] = null) {
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

    log.info("Smart Rebalance triggered. Load: %s. Target: %s", myLoad(), evenDistribution())

    if (myLoad() > evenDistribution())
      drainToLoad(evenDistribution().longValue)
  }

  /**
   * Performs a simple rebalance. Target load is set to (# of work items / node count).
   */
  private def simpleRebalance(data: Option[Array[Byte]] = null) {
    val nodeCount = nodes.synchronized(nodes.size)
    val totalUnits = allWorkUnits.size
    val fairShare = (totalUnits.toDouble / nodeCount).ceil.toInt

    log.info("Simple Rebalance triggered. Total work units: %s. Nodes: %s. My load: %s. " +
      "Target: %s", totalUnits, nodeCount, myWorkUnits.size, fairShare)

    if (myWorkUnits.size > fairShare)
      drainToCount(fairShare)
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
   * the cluster. This is determined by the total load divided by the number of nodes.
   */
  private def evenDistribution() : Double = {
    loadMap.values.sum / nodes.synchronized(nodes.size)
  }


  /**
   * Utility method for converting an array of bytes to a string.
   */
  private def bytesToString(bytes: Array[Byte]) : String = {
    new String(bytes, Charset.forName("UTF-8"))
  }

  /**
   * Utility method for converting an array of bytes to a double.
   */
  private def bytesToDouble(bytes: Array[Byte]) : Double = {
    bytesToString(bytes).toDouble
  }

}
