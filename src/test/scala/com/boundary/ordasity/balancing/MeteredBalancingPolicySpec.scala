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

import org.junit.Test
import com.boundary.ordasity._
import collection.JavaConversions._
import org.apache.zookeeper.ZooKeeper
import com.twitter.common.zookeeper.ZooKeeperClient
import java.util.concurrent.ScheduledFuture
import com.yammer.metrics.scala.Meter
import java.util.{LinkedList, HashMap, UUID}
import com.simple.simplespec.Spec

class MeteredBalancingPolicySpec extends Spec {

  val config = new ClusterConfig().
    setNodeId("testNode").
    setRebalanceInterval(1).
    setDrainTime(1).
    setHosts("no_existe:2181").
    setAutoRebalance(false)

  class `Metered Balancing Policy` {

    @Test def `throw when initialize w/wrong listener` {
      val cluster = new Cluster(UUID.randomUUID.toString, mock[ClusterListener], config)
      val balancer = new MeteredBalancingPolicy(cluster, config)

      val threw = try {
        balancer.init()
        false
      } catch {
        case e: Exception => true
      }

      threw.must(be(true))
    }

    @Test def `not throw when initialized w/wrong listener` {
      val cluster = makeCluster()
      val balancer = new MeteredBalancingPolicy(cluster, config)

      val threw = try {
        balancer.init()
        false
      } catch {
        case e: Exception => true
      }

      threw.must(be(false))
    }

    @Test def `even distribution` {
      val cluster = makeCluster()
      val balancer = new MeteredBalancingPolicy(cluster, config)

      cluster.loadMap = new HashMap[String, Double]
      cluster.loadMap.putAll(
        Map("foo" -> 100.0, "bar" -> 200.0, "baz" -> 300.0))

      balancer.evenDistribution().must(equal(300.0))
    }

    @Test def `my load` {
      val cluster = makeCluster()
      val balancer = new MeteredBalancingPolicy(cluster, config)

      cluster.loadMap = new HashMap[String, Double]
      cluster.loadMap.putAll(
        Map("foo" -> 100.0, "bar" -> 200.0, "baz" -> 300.0))

      cluster.myWorkUnits.add("foo")
      cluster.myWorkUnits.add("bar")
      balancer.myLoad().must(be(300.0))
    }

    @Test def `shutdown` {
      val cluster = makeCluster()
      val balancer = new MeteredBalancingPolicy(cluster, config)

      val mockFuture = mock[ScheduledFuture[_]]
      balancer.loadFuture = Some(mockFuture)

      balancer.shutdown()
      verify.one(mockFuture).cancel(true)
    }

    @Test def `on shutdown work` {
      val cluster = makeCluster()
      val balancer = new MeteredBalancingPolicy(cluster, config)
      cluster.balancingPolicy = balancer

      balancer.meters.put("foo", mock[Meter])
      balancer.onShutdownWork("foo")
      balancer.meters.contains("foo").must(be(false))
    }

    @Test def `claim work` {
      val cluster = makeCluster()
      val balancer = new MeteredBalancingPolicy(cluster, config)
      cluster.balancingPolicy = balancer

      Map("foo" -> 100.0, "bar" -> 200.0, "baz" -> 300.0).foreach(el =>
        cluster.allWorkUnits.put(el._1, ""))

      cluster.loadMap = new HashMap[String, Double]
      cluster.loadMap.putAll(
        Map("foo" -> 100.0, "bar" -> 200.0, "baz" -> 300.0))

      // Simulate all "create" requests succeeding.
      cluster.zk.get().create(any, any, any, any).returns("")
      balancer.claimWork()

      (balancer.myLoad() >= balancer.evenDistribution()).must(be(true))
      cluster.myWorkUnits.size().must(be(lessThan(3)))
    }

    @Test def `rebalance` {
      val cluster = makeCluster()
      val balancer = new MeteredBalancingPolicy(cluster, config)
      cluster.balancingPolicy = balancer

      val map = Map("foo" -> 100.0, "bar" -> 200.0, "baz" -> 300.0)

      map.foreach(el =>
        cluster.allWorkUnits.put(el._1, ""))
      cluster.myWorkUnits.addAll(map.keySet)
      cluster.loadMap = new HashMap[String, Double]
      cluster.loadMap.putAll(map)

      balancer.rebalance()
      
      Thread.sleep(1200)

      (balancer.myLoad() >= balancer.evenDistribution()).must(be(true))
      cluster.myWorkUnits.size().must(be(lessThan(3)))
    }

    @Test def `drain to load` {
      val cluster = makeCluster()
      val balancer = new MeteredBalancingPolicy(cluster, config)
      cluster.balancingPolicy = balancer

      val map = Map("foo" -> 100.0, "bar" -> 200.0, "baz" -> 300.0)

      map.foreach(el =>
        cluster.allWorkUnits.put(el._1, ""))
      cluster.myWorkUnits.addAll(map.keySet)
      cluster.loadMap = new HashMap[String, Double]
      cluster.loadMap.putAll(map)
      balancer.myLoad().must(be(600.0))
      balancer.drainToLoad(balancer.evenDistribution().longValue())

      Thread.sleep(1200)

      (balancer.myLoad() >= balancer.evenDistribution()).must(be(true))

      balancer.myLoad().must(be(500.0))
    }

    @Test def `build drain task` {
      val cluster = makeCluster()
      val balancer = new MeteredBalancingPolicy(cluster, config)
      cluster.balancingPolicy = balancer

      val map = Map("foo" -> 100.0, "bar" -> 200.0, "baz" -> 300.0)

      map.foreach(el =>
        cluster.allWorkUnits.put(el._1, ""))
      cluster.myWorkUnits.addAll(map.keySet)
      cluster.loadMap = new HashMap[String, Double]
      cluster.loadMap.putAll(map)
      balancer.myLoad().must(be(600.0))

      val drainList = new LinkedList[String]
      drainList.addAll(map.keySet.toList)
      val task = balancer.buildDrainTask(drainList, 10, false, balancer.myLoad())

      task.run()
      Thread.sleep(1200)

      (balancer.myLoad() == balancer.evenDistribution()).must(be(true))
      balancer.myLoad().must(be(300.0))
    }
  }

  def makeCluster() : Cluster = {
    val listener = new SmartListener {
      def startWork(workUnit: String, meter: Meter) = null
      def shutdownWork(workUnit: String) = null
      def onLeave() = null
      def onJoin(client: ZooKeeperClient) = null
    }

    val cluster = new Cluster(UUID.randomUUID.toString, listener, config)
    val mockZK = mock[ZooKeeper]
    val mockZKClient = mock[ZooKeeperClient]
    mockZKClient.get().returns(mockZK)
    cluster.zk = mockZKClient

    cluster.nodes = new HashMap[String, NodeInfo]
    cluster.allWorkUnits = new HashMap[String, String]
    cluster.workUnitMap = new HashMap[String, String]
    cluster.handoffRequests = new HashMap[String, String]
    cluster.handoffResults = new HashMap[String, String]
    cluster.nodes.put("foo", NodeInfo(NodeState.Started.toString, 0L))
    cluster.nodes.put("bar", NodeInfo(NodeState.Started.toString, 0L))
    cluster.nodes.put("baz", NodeInfo(NodeState.Draining.toString, 0L))
    cluster
  }

}
