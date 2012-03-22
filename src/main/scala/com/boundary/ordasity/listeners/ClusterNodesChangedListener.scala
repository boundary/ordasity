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

import com.codahale.logula.Logging
import collection.JavaConversions._
import com.boundary.ordasity.{Cluster, NodeInfo}
import com.twitter.common.zookeeper.ZooKeeperMap

/**
 * As the nodes in an Ordasity cluster come, go, or change state, we must update
 * our internal view of the cluster's topology, then claim work and verify the
 * integrity of existing mappings as appropriate.
 */
class ClusterNodesChangedListener(cluster: Cluster)
    extends ZooKeeperMap.ZKMapListener[NodeInfo] with Logging {

  def nodeChanged(nodeName: String, data: NodeInfo) {
    if (!cluster.initialized.get()) return

    log.info("Nodes: %s".format(cluster.nodes.map(n => n._1).mkString(", ")))
    cluster.claimWork()
    cluster.verifyIntegrity()
  }

  def nodeRemoved(nodeName: String) {
    if (!cluster.initialized.get()) return
    log.info("%s has left the cluster.", nodeName)
    cluster.claimWork()
    cluster.verifyIntegrity()
  }
}
