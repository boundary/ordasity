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

import java.util.Random
import java.util.concurrent.CountDownLatch
import com.boundary.ordasity.{Cluster, ClusterConfig, SmartListener}
import com.codahale.logula.Logging
import com.yammer.metrics.Meter
import com.twitter.zookeeper.ZooKeeperClient
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit, ScheduledFuture}
import java.util.{HashMap, TimerTask}

Logging.configure

val random = new Random()
val latch = new CountDownLatch(1)
val pool = new ScheduledThreadPoolExecutor(1)

val futures = new HashMap[String, ScheduledFuture[_]]

val config = new ClusterConfig("localhost:2181").
  setAutoRebalance(true).
  setRebalanceInterval(60 * 5).
  useSmartBalancing(true).
  setDrainTime(60).
  setZKTimeout(3000).
  setUseSoftHandoff(true).
  setNodeId(java.util.UUID.randomUUID().toString)

val listener = new SmartListener {
  def onJoin(client: ZooKeeperClient) = {}
  def onLeave() = { }

  // Do yer thang, mark dat meter.
  def startWork(workUnit: String, meter: Meter) = {
    val task = new TimerTask {
      def run() = meter.mark(random.nextInt(1000))
    }
	  val future = pool.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS)
	  futures.put(workUnit, future)
  }

  // Stop doin' that thang
  def shutdownWork(workUnit: String) {
    futures.get(workUnit).cancel(true)
  }
}

val clustar = new Cluster("example_service", listener, config)

clustar.join()
latch.await()
