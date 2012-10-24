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

import com.yammer.metrics.core.Gauge
import com.yammer.metrics.scala.Meter
import com.twitter.common.zookeeper.ZooKeeperClient

abstract class Listener {
  def onJoin(client: ZooKeeperClient)
  def onLeave()
  def shutdownWork(workUnit: String)
}

abstract class SmartGaugedListener extends Listener {
  def startWork(workUnit: String)
  def workload(workUnit: String): Double
}

abstract class SmartListener extends Listener {
  def startWork(workUnit: String, meter: Meter)
}

abstract class ClusterListener extends Listener {
  def startWork(workUnit: String)
}

