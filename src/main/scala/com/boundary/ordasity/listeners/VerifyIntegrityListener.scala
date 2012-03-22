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
import com.twitter.common.zookeeper.ZooKeeperMap
import com.boundary.ordasity.{ClusterConfig, Cluster}

/**
 * As work units distributed about the cluster change, we must verify the
 * integrity of this node's mappings to ensure it matches reality, and attempt
 * to claim work if the topology of nodes and work units in the cluster has changed.
 */
class VerifyIntegrityListener(cluster: Cluster, config: ClusterConfig)
    extends ZooKeeperMap.ZKMapListener[String] with Logging {

  def nodeChanged(nodeName: String, data: String) {
    if (!cluster.initialized.get()) return

    log.debug(config.workUnitName.capitalize +
      " IDs: %s".format(cluster.allWorkUnits.keys.mkString(", ")))

    cluster.claimWork()
    cluster.verifyIntegrity()
  }

  def nodeRemoved(nodeName: String) {
    if (!cluster.initialized.get()) return

    cluster.claimWork()
    cluster.verifyIntegrity()
  }
}
