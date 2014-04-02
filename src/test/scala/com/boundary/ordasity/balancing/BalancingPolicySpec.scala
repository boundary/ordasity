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
import java.util.{HashMap, UUID}
import collection.JavaConversions._
import org.apache.zookeeper.ZooKeeper
import com.twitter.common.zookeeper.ZooKeeperClient
import org.mockito.Mockito
import org.apache.zookeeper.KeeperException.NodeExistsException
import com.simple.simplespec.Spec
import org.apache.zookeeper.data.Stat
import com.google.common.base.Charsets


class DummyBalancingPolicy(cluster: Cluster, config: ClusterConfig)
    extends BalancingPolicy(cluster, config) {
  def claimWork() = null
  def rebalance() = null
}

class BalancingPolicySpec extends Spec {

  val config = ClusterConfig.builder().
    setNodeId("testNode").
    setAutoRebalanceInterval(1).
    setDrainTime(1).
    setHosts("no_existe:2181").
    setEnableAutoRebalance(false).build()

  class `Base Balancing Policy Tests` {
    
    @Test def `active node size` {
      val cluster = makeCluster()
      val balancer = new DummyBalancingPolicy(cluster, config)

      cluster.nodes.put("foo", NodeInfo(NodeState.Fresh.toString, 0L))
      cluster.nodes.put("bar", NodeInfo(NodeState.Shutdown.toString, 0L))
      cluster.nodes.put("baz", NodeInfo(NodeState.Started.toString, 0L))
      cluster.nodes.put("taco", NodeInfo(NodeState.Draining.toString, 0L))
      cluster.nodes.put("nacho", NodeInfo("how did i get here?", 0L))

      balancer.activeNodeSize().must(be(1))
    }

    @Test def `is fair game` {
      val cluster = makeCluster()
      val balancer = new DummyBalancingPolicy(cluster, config)

      cluster.allWorkUnits.put("vanilla", "")
      cluster.allWorkUnits.put("bean", "c'eci nes pas json")
      cluster.allWorkUnits.put("peggedToMe", """{"%s": "%s"}""".format(cluster.name, cluster.myNodeID))
      cluster.allWorkUnits.put("peggedToOther", """{"%s": "%s"}""".format(cluster.name, "otherNode"))

      balancer.isFairGame("vanilla").must(be(true))
      balancer.isFairGame("bean").must(be(true))
      balancer.isFairGame("peggedToMe").must(be(true))
      balancer.isFairGame("peggedToOther").must(be(false))
    }

    @Test def `is pegged to me` {
      val cluster = makeCluster()
      val balancer = new DummyBalancingPolicy(cluster, config)

      cluster.allWorkUnits.put("vanilla", "")
      cluster.allWorkUnits.put("bean", "c'eci nes pas json")
      cluster.allWorkUnits.put("peggedToMe", """{"%s": "%s"}""".format(cluster.name, cluster.myNodeID))
      cluster.allWorkUnits.put("peggedToOther", """{"%s": "%s"}""".format(cluster.name, "otherNode"))

      balancer.isPeggedToMe("vanilla").must(be(false))
      balancer.isPeggedToMe("bean").must(be(false))
      balancer.isPeggedToMe("peggedToMe").must(be(true))
      balancer.isPeggedToMe("peggedToOther").must(be(false))
    }

    @Test def `attempt to claim` {
      val cluster = makeCluster()
      val balancer = new DummyBalancingPolicy(cluster, config)

      Mockito.when(cluster.zk.get().create(any, any, any, any)).
        thenReturn("").
        thenThrow(new NodeExistsException)

      balancer.attemptToClaim("taco")
      cluster.myWorkUnits.contains("taco").must(be(true))

      balancer.attemptToClaim("fajita")
      cluster.myWorkUnits.contains("fajita").must(be(false))
    }

    @Test def `claim work pegged to me` {
      val cluster = makeCluster()
      val balancer = new DummyBalancingPolicy(cluster, config)
      cluster.allWorkUnits.put("peggedToMe", """{"%s": "%s"}""".format(cluster.name, cluster.myNodeID))

      Mockito.when(cluster.zk.get().create(any, any, any, any)).
        thenThrow(new NodeExistsException).
        thenReturn("")

      balancer.attemptToClaim("peggedToMe")
      cluster.myWorkUnits.contains("peggedToMe").must(be(true))
    }

    @Test def `drain to count` {
      val cluster = makeCluster()
      val balancer = new DummyBalancingPolicy(cluster, config)

      val workUnits = List("one", "two", "three", "four", "five", "six", "seven")
      cluster.myWorkUnits.addAll(workUnits)
      workUnits.foreach(el => cluster.allWorkUnits.put(el, ""))
      workUnits.foreach(el => cluster.workUnitMap.put(el, "testNode"))
      workUnits.foreach(el =>
        cluster.zk.get().getData(equalTo(cluster.workUnitClaimPath(el)), any[Boolean], any[Stat])
          .returns(cluster.myNodeID.getBytes(Charsets.UTF_8))
      )

      balancer.drainToCount(0)

      Thread.sleep(1100)
      cluster.myWorkUnits.size().must(be(0))
      cluster.state.get().must(be(NodeState.Started))
    }

    @Test def `drain to count, ensuring that work units pegged to the node are not shut down` {
      val cluster = makeCluster()
      val balancer = new DummyBalancingPolicy(cluster, config)

      val workUnits = List("one", "two", "three", "four", "five", "six", "seven")
      cluster.myWorkUnits.addAll(workUnits)
      cluster.workUnitsPeggedToMe.add("two")
      workUnits.foreach(el => cluster.allWorkUnits.put(el, ""))
      workUnits.foreach(el => cluster.workUnitMap.put(el, "testNode"))
      workUnits.foreach(el =>
        cluster.zk.get().getData(equalTo(cluster.workUnitClaimPath(el)), any[Boolean], any[Stat])
          .returns(cluster.myNodeID.getBytes(Charsets.UTF_8))
      )

      balancer.drainToCount(0)

      Thread.sleep(1100)
      cluster.myWorkUnits.size().must(be(1))
      cluster.state.get().must(be(NodeState.Started))
    }

    @Test def `drain to zero with shutdown, ensuring that work units pegged to the node are shut down` {
      val cluster = makeCluster()
      val balancer = new DummyBalancingPolicy(cluster, config)

      val workUnits = List("one", "two", "three", "four", "five", "six", "seven")
      cluster.myWorkUnits.addAll(workUnits)
      cluster.workUnitsPeggedToMe.add("two")
      workUnits.foreach(el => cluster.allWorkUnits.put(el, ""))
      workUnits.foreach(el => cluster.workUnitMap.put(el, "testNode"))
      workUnits.foreach(el =>
        cluster.zk.get().getData(equalTo(cluster.workUnitClaimPath(el)), any[Boolean], any[Stat])
          .returns(cluster.myNodeID.getBytes(Charsets.UTF_8))
      )

      balancer.drainToCount(0, doShutdown = true)

      Thread.sleep(1100)
      cluster.myWorkUnits.size().must(be(0))
      cluster.state.get().must(be(NodeState.Shutdown))
    }


    @Test def `drain to count and shutdown` {
      val cluster = makeCluster()
      val balancer = new DummyBalancingPolicy(cluster, config)

      val workUnits = List("one", "two", "three", "four", "five", "six", "seven")
      cluster.myWorkUnits.addAll(workUnits)
      workUnits.foreach(el => cluster.allWorkUnits.put(el, ""))
      workUnits.foreach(el => cluster.workUnitMap.put(el, "testNode"))
      workUnits.foreach(el =>
        cluster.zk.get().getData(equalTo(cluster.workUnitClaimPath(el)), any[Boolean], any[Stat])
          .returns(cluster.myNodeID.getBytes(Charsets.UTF_8))
      )

      balancer.drainToCount(0, doShutdown = true)

      Thread.sleep(1100)
      cluster.myWorkUnits.size().must(be(0))
      cluster.state.get().must(be(NodeState.Shutdown))
    }

    @Test def `drain to count with handoff` {
      val cluster = makeCluster()
      val balancer = new DummyBalancingPolicy(cluster, config)

      val workUnits = List("one", "two", "three", "four", "five", "six", "seven")
      cluster.myWorkUnits.addAll(workUnits)
      workUnits.foreach(el => cluster.allWorkUnits.put(el, ""))
      workUnits.foreach(el => cluster.workUnitMap.put(el, "testNode"))
      workUnits.foreach(el =>
        cluster.zk.get().getData(equalTo(cluster.workUnitClaimPath(el)), any[Boolean], any[Stat])
          .returns(cluster.myNodeID.getBytes(Charsets.UTF_8))
      )

      cluster.zk.get().create(any, any, any, any).returns("")
      balancer.drainToCount(3, useHandoff = true)
      Thread.sleep(2100)
      verify.exactly(4)(cluster.zk.get()).create(any, any, any, any)
      cluster.state.get().must(be(NodeState.Started))
    }

    @Test def `get unclaimed` {
      val cluster = makeCluster()
      val balancer = new CountBalancingPolicy(cluster, config)

      val workUnits = List("one", "two", "three", "four", "five", "six", "seven", "eight")
      workUnits.foreach(el => cluster.allWorkUnits.put(el, ""))
      cluster.myWorkUnits.add("eight")

      List("one", "two").foreach(el => cluster.workUnitMap.put(el, ""))
      List("three", "four").foreach(el => cluster.handoffRequests.put(el, ""))
      List("five", "six").foreach(el => cluster.handoffResults.put(el, ""))

      balancer.getUnclaimed().must(be(Set("three", "four", "seven")))
    }
  }

  def makeCluster() : Cluster = {
    val cluster = new Cluster(UUID.randomUUID.toString, mock[ClusterListener], config)

    val mockZK = mock[ZooKeeper]
    val mockZKClient = mock[ZooKeeperClient]
    mockZKClient.get().returns(mockZK)
    cluster.zk = mockZKClient

    cluster.state.set(NodeState.Started)
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
