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
import com.boundary.ordasity.{ClusterConfig, Cluster}

/**
 * Ordasity's count-based load balancing policy is simple. A node in the cluster
 * will attempt to claim (<n> work work units / <k> nodes + 1) work units. It may
 * be initialized with either a simple ClusterListener or a metered SmartListener.
 */
class CountBalancingPolicy(cluster: Cluster, config: ClusterConfig) extends BalancingPolicy(cluster, config) {

  /**
    * Claims work in Zookeeper. This method will attempt to divide work about the cluster
    * by claiming up to ((<x> Work Unit Count / <y> Nodes) + 1) work units. While
    * this doesn't necessarily represent an even load distribution based on work unit load,
    * it should result in a relatively even "work unit count" per node. This randomly-distributed
    * amount is in addition to any work units which are pegged to this node.
   */
  def claimWork() {
    var claimed = cluster.myWorkUnits.size
    val nodeCount = activeNodeSize()

    cluster.allWorkUnits.synchronized {
      val maxToClaim = getMaxToClaim(nodeCount)

      log.debug("%s Nodes: %s. %s: %s.", cluster.name, nodeCount, config.workUnitName.capitalize, cluster.allWorkUnits.size)
      log.debug("Claiming %s pegged to me, and up to %s more.", config.workUnitName, maxToClaim)

      val unclaimed = getUnclaimed()
      log.debug("Handoff requests: %s, Handoff Results: %s, Unclaimed: %s",
        cluster.handoffRequests.mkString(", "), cluster.handoffResults.mkString(", "), unclaimed.mkString(", "))

      for (workUnit <- unclaimed) {
        if ((isFairGame(workUnit) && claimed < maxToClaim) || isPeggedToMe(workUnit)) {
          if (config.useSoftHandoff && cluster.handoffRequests.contains(workUnit) && attemptToClaim(workUnit, true)) {
            log.info("Accepted handoff of %s.", workUnit)
            claimed += 1
          } else if (!cluster.handoffRequests.contains(workUnit) && attemptToClaim(workUnit)) {
            claimed += 1
          }
        }
      }
    }
  }

  /**
   * Determines the maximum number of work units the policy should attempt to claim.
   */
  def getMaxToClaim(nodeCount: Int) : Int = cluster.allWorkUnits.synchronized {
    if (cluster.allWorkUnits.size <= 1) cluster.allWorkUnits.size
    else (cluster.allWorkUnits.size / nodeCount.toDouble).ceil.intValue()
  }
  

  /**
   * Performs a simple rebalance. Target load is set to (# of work items / node count).
   */
  def rebalance() {
    val target = fairShare()

    if (cluster.myWorkUnits.size > target) {
      log.info("Simple Rebalance triggered. My Share: %s. Target: %s.", cluster.myWorkUnits.size, target)
      super.drainToCount(target)
    }
  }

  /**
   * Determines the fair share of work units this node should claim.
   */
  def fairShare() : Int = {
    (cluster.allWorkUnits.size.toDouble / activeNodeSize()).ceil.toInt
  }

}
