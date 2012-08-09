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

import overlock.atomicmap.AtomicMap
import com.boundary.ordasity._
import com.yammer.metrics.scala.Meter

/**
 * A metered balancing policy is just a gauged balancing policy where the gauge
 * value is calculated from the meter's one-minute rate. It must be initialized
 * with a metered SmartListener.
 */
class MeteredBalancingPolicy(cluster: Cluster, config: ClusterConfig)
    extends GaugedBalancingPolicy(cluster, config) {

  val persistentMeterCache = AtomicMap.atomicNBHM[String, Meter]

  override def init() : BalancingPolicy = {
    if (!cluster.listener.isInstanceOf[SmartListener]) {
      throw new RuntimeException("Ordasity's metered balancing policy must be initialized with " +
        "a SmartListener, but you provided something else. Please fix that so we can tick " +
        "the meter as your application performs work!")
    }

    this
  }

}
