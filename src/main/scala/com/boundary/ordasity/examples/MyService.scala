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

import com.yammer.metrics.scala.Meter
import com.twitter.common.zookeeper.ZooKeeperClient

class MyService {
  val config = new ClusterConfig().
    setHosts("localhost:2181").
    setAutoRebalance(true).
    setRebalanceInterval(60).
    setUseSmartBalancing(true).
    setDrainTime(60).
    setZKTimeout(3000)

  val cluster = new Cluster("ServiceName", listener, config)

  val listener = new SmartListener {
    def onJoin(client: ZooKeeperClient) = null // You are *in*, baby.

    def startWork(workUnit: String, meter: Meter) = null // Do yer thang, mark dat meter.

    def shutdownWork(workUnit: String) = null // Stop doin' that thang

    def onLeave() = null
  }

  cluster.join()
}
