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

package com.boundary.ordasity.listeners

import com.boundary.ordasity._
import com.codahale.logula.Logging
import java.util.concurrent.TimeUnit
import com.twitter.common.zookeeper.ZooKeeperMap
import org.apache.zookeeper.{WatchedEvent, Watcher}

/* The HandoffResultsListener keeps track of the handoff state of work units
 * around the cluster. As events fire, this listener determines whether or not
 * the current node is offering handoff of a work unit or accepting it, and
 * managing that lifecycle as appropriate.
 */
class HandoffResultsListener(cluster: Cluster, config: ClusterConfig)
    extends ZooKeeperMap.ZKMapListener[String] with Logging {

  def nodeChanged(nodeName: String, data: String) = apply(nodeName)
  def nodeRemoved(nodeName: String) = apply(nodeName)

  /**
   * If I am the node which accepted this handoff, finish the job.
   * If I'm the node that requested to hand off this work unit to
   * another node, shut it down after <config> seconds.
   */
  def apply(workUnit: String) {
    if (!cluster.initialized.get()) return

    if (iRequestedHandoff(workUnit)) {
      log.info("Handoff of %s to %s completed. Shutting down %s in %s seconds.", workUnit,
        cluster.getOrElse(cluster.handoffResults, workUnit, "(None)"), workUnit, config.handoffShutdownDelay)
      ZKUtils.delete(cluster.zk, "/%s/handoff-requests/%s".format(cluster.name, workUnit))
      cluster.pool.get.schedule(shutdownAfterHandoff(workUnit), config.handoffShutdownDelay, TimeUnit.SECONDS)
    }
  }

  /**
   * Determines if this Ordasity node requested handoff of a work unit to someone else.
   * I have requested handoff of a work unit if it's currently a member of my active set
   * and its destination node is another node in the cluster.
   */
  def iRequestedHandoff(workUnit: String) : Boolean = {
    val destinationNode = cluster.getOrElse(cluster.handoffResults, workUnit, "")
    cluster.myWorkUnits.contains(workUnit) && !destinationNode.equals("") &&
      !cluster.isMe(destinationNode)
  }

  /**
   * Builds a runnable to shut down a work unit after a configurable delay once handoff
   * has completed. If the cluster has been instructed to shut down and the last work unit
   * has been handed off, this task also directs this Ordasity instance to shut down.
   */
  def shutdownAfterHandoff(workUnit: String) : Runnable = {
    new Runnable {
      def run() {
        log.info("Shutting down %s following handoff to %s.",
          workUnit, cluster.getOrElse(cluster.handoffResults, workUnit, "(None)"))
        cluster.shutdownWork(workUnit, doLog = false, deleteZNode = true)

        if (cluster.myWorkUnits.size() == 0 && cluster.state.get() == NodeState.Draining)
          cluster.shutdown()
      }
    }
  }

  /**
   * Completes the process of handing off a work unit from one node to the current one.
   * Attempts to establish a final claim to the node handed off to me in ZooKeeper, and
   * repeats execution of the task every two seconds until it is complete.
   */
  def finishHandoff(workUnit: String, retryTime: Int = 2000) {
    log.info("Handoff of %s to me acknowledged. Deleting claim ZNode for %s and waiting for %s to " +
      "shutdown work.", workUnit, workUnit, cluster.getOrElse(cluster.workUnitMap, workUnit, "(None)"))

    val path = "/%s/claimed-%s/%s".format(cluster.name, config.workUnitShortName, workUnit)
    val completeHandoff = () => {
      try {
        log.info("Completing handoff of %s", workUnit)
        if (ZKUtils.createEphemeral(cluster.zk, path, cluster.myNodeID) || cluster.znodeIsMe(path)) {
          log.info("Handoff of %s to me complete. Peer has shut down work.", workUnit)
        }
        else {
          log.warn("Failed to completed handoff of %s - couldn't create ephemeral node", workUnit)
        }
      } catch {
        case e: Exception =>
          log.error(e, "Error completing handoff of %s to me.", workUnit)
      } finally {
        ZKUtils.delete(cluster.zk, "/" + cluster.name + "/handoff-result/" + workUnit)
        cluster.claimedForHandoff.remove(workUnit)
      }
    }

    val stat = cluster.zk.get().exists(path, new Watcher {
      def process(event: WatchedEvent) {
        // Don't really care about the type of event here - call unconditionally to clean up state
        completeHandoff()
      }
    })
    // Unlikely that peer will have already deleted znode, but handle it regardless
    if (stat == null) {
      log.warn("Peer already deleted znode of %s", workUnit)
      completeHandoff()
    }
  }

}
