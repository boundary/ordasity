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

package com.boundary.ordasity.balancing

import collection.JavaConversions._
import overlock.atomicmap.AtomicMap
import com.boundary.ordasity._
import java.util.concurrent.{TimeUnit, ScheduledFuture}
import com.yammer.metrics.scala.Meter
import java.util.{TimerTask, LinkedList}
import org.apache.zookeeper.CreateMode

/**
 * Ordasity's count-based load balancing policy is simple. A node in the cluster
 * will attempt to claim (<n> work work units / <k> nodes + 1) work units. It may
 * be initialized with either a simple ClusterListener or a metered SmartListener.
 */
class MeteredBalancingPolicy(cluster: Cluster, config: ClusterConfig)
    extends BalancingPolicy(cluster, config) {

  val meters = AtomicMap.atomicNBHM[String, Meter]
  val persistentMeterCache = AtomicMap.atomicNBHM[String, Meter]
  val loadGauge = metrics.gauge[Double]("my_load") { myLoad() }
  var loadFuture : Option[ScheduledFuture[_]] = None

  override def init() : BalancingPolicy = {
    if (!cluster.listener.isInstanceOf[SmartListener]) {
      throw new RuntimeException("Ordasity's metered balancing policy must be initialized with " +
        "a SmartListener, but you provided a simple listener. Please flip that so we can tick " +
        "the meter as your application performs work!")
    }

    this
  }

  /**
   * Begins by claimng all work units that are pegged to this node.
   * Then, continues to claim work from the available pool until we've claimed
   * equal to or slightly more than the total desired load.
   */
  def claimWork() {
    cluster.allWorkUnits.synchronized {
      for (workUnit <- getUnclaimed())
        if (isPeggedToMe(workUnit))
          claimWorkPeggedToMe(workUnit)

      val unclaimed = new LinkedList[String](getUnclaimed())
      while (myLoad() <= evenDistribution && !unclaimed.isEmpty) {
        val workUnit = unclaimed.poll()

        if (config.useSoftHandoff && cluster.handoffRequests.contains(workUnit)
            && isFairGame(workUnit) && attemptToClaim(workUnit, claimForHandoff = true)) {
              log.info("Accepted handoff for %s.", workUnit)
              cluster.handoffResultsListener.finishHandoff(workUnit)
            }

        else if (isFairGame(workUnit))
          attemptToClaim(workUnit)
      }
    }
  }

  /**
   * Performs a "smart rebalance." The target load is set to (cluster load / node count),
   * where "load" is determined by the sum of all work unit meters in the cluster.
   */
  def rebalance() {
    val target = evenDistribution()
    if (myLoad() > target) {
      log.info("Smart Rebalance triggered. Load: %s. Target: %s", myLoad(), target)
      drainToLoad(target.longValue)
    }
  }


  /**
   * When smart balancing is enabled, calculates the even distribution of load about
   * the cluster. This is determined by the total load divided by the number of alive nodes.
   */
  def evenDistribution() : Double = {
    cluster.loadMap.values.sum / activeNodeSize().doubleValue()
  }


  /**
   * Determines the current load on this instance when smart rebalancing is enabled.
   * This load is determined by the sum of all of this node's meters' one minute rate.
   */
  def myLoad() : Double = {
    var load = 0d
    log.debug(cluster.loadMap.toString)
    log.debug(cluster.myWorkUnits.toString)
    cluster.myWorkUnits.foreach(u => load += cluster.getOrElse(cluster.loadMap, u, 0))
    load
  }

  /**
   * Once a minute, pass off information about the amount of load generated per
   * work unit off to Zookeeper for use in the claiming and rebalancing process.
   */
  private def scheduleLoadTicks() {
    val sendLoadToZookeeper = new Runnable {
      def run() {
        try {
          meters.foreach { case(workUnit, meter) =>
            val loadPath = "/%s/meta/workload/%s".format(cluster.name, workUnit)
            ZKUtils.setOrCreate(cluster.zk, loadPath, meter.oneMinuteRate.toString, CreateMode.PERSISTENT)
          }

          val myInfo = new NodeInfo(cluster.getState.toString, cluster.zk.get().getSessionId)
          val nodeLoadPath = "/%s/nodes/%s".format(cluster.name, cluster.myNodeID)
          val myInfoEncoded = JsonUtils.OBJECT_MAPPER.writeValueAsString(myInfo)
          ZKUtils.setOrCreate(cluster.zk, nodeLoadPath, myInfoEncoded, CreateMode.EPHEMERAL)

          log.info("My load: %s", myLoad())          
        } catch {
          case e: Exception => log.error(e, "Error reporting load info to ZooKeeper.")
        }
      }
    }

    loadFuture = Some(cluster.pool.get.scheduleAtFixedRate(
      sendLoadToZookeeper, 0, 1, TimeUnit.MINUTES))
  }


  /**
   * Drains excess load on this node down to a fraction distributed across the cluster.
   * The target load is set to (clusterLoad / # nodes).
   */
  def drainToLoad(targetLoad: Long, time: Int = config.drainTime,
                          useHandoff: Boolean = config.useSoftHandoff) {
    val startingLoad = myLoad()
    var currentLoad = myLoad()
    val drainList = new LinkedList[String]
    val eligibleToDrop = new LinkedList[String](cluster.myWorkUnits -- cluster.workUnitsPeggedToMe)

    while (currentLoad > targetLoad && !eligibleToDrop.isEmpty) {
      val workUnit = eligibleToDrop.poll()
      var workUnitLoad : Double = cluster.getOrElse(cluster.loadMap, workUnit, 0)

      if (workUnitLoad > 0 && (currentLoad - workUnitLoad) > targetLoad) {
        drainList.add(workUnit)
        currentLoad -= workUnitLoad
      }
    }

    val drainInterval = ((config.drainTime.toDouble / drainList.size) * 1000).intValue()
    val drainTask = buildDrainTask(drainList, drainInterval, useHandoff, currentLoad)

    if (!drainList.isEmpty) {
      log.info("Releasing work units over %s seconds. Current load: %s. Target: %s. " +
        "Releasing: %s", time, startingLoad, targetLoad, drainList.mkString(", "))
      cluster.pool.get.schedule(drainTask, 0, TimeUnit.SECONDS)
    }
  }

  def buildDrainTask(drainList: LinkedList[String], drainInterval: Int, useHandoff: Boolean,
      currentLoad: Double) : TimerTask = {
    new TimerTask {
      def run() {
        if (drainList.isEmpty || myLoad <= evenDistribution) {
          log.info("Finished the drain list, or my load is now less than an even distribution. " +
            "Stopping rebalance. Remaining work units: %s", drainList.mkString(", "))
          return
        }
        else if (useHandoff)
          cluster.requestHandoff(drainList.poll)
        else
          cluster.shutdownWork(drainList.poll)

        cluster.pool.get.schedule(this, drainInterval, TimeUnit.MILLISECONDS)
      }
    }
  }

  override def onConnect() = scheduleLoadTicks()

  override def shutdown() {
    if (loadFuture.isDefined)
      loadFuture.get.cancel(true)
  }

  override def onShutdownWork(workUnit: String) =
    meters.remove(workUnit)

}
