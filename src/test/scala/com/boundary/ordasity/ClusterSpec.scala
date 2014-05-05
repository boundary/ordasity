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

import collection.JavaConversions._
import balancing.{CountBalancingPolicy, BalancingPolicy}
import org.junit.Test
import java.util.{UUID, HashMap}
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.ZooDefs.Ids
import com.twitter.common.zookeeper.{ZooKeeperMap, ZooKeeperClient}
import org.apache.zookeeper.{Watcher, CreateMode, ZooKeeper}
import com.simple.simplespec.Spec
import com.google.common.base.Charsets
import org.mockito.invocation.InvocationOnMock
import org.apache.zookeeper.KeeperException.NoNodeException
import com.fasterxml.jackson.databind.node.ObjectNode

class ClusterSpec extends Spec {

  val id = UUID.randomUUID().toString
  val config = ClusterConfig.builder().
    setNodeId("testNode").
    setAutoRebalanceInterval(1).
    setDrainTime(1).
    setHosts("no_existe:2181").build()

  val mockClusterListener = mock[Listener]
  val cluster = new Cluster(id, mockClusterListener, config)

  class `Test Cluster` {

    @Test def `previous ZK session still active` {
      val nodeInfo = NodeInfo(NodeState.Started.toString, 101L)

      val mockZKClient = mock[ZooKeeper]
      mockZKClient.getSessionId.returns(101L)
      mockZKClient.getData(equalTo("/%s/nodes/testNode".format(id)), any[Boolean], any[Stat]).
        returns(JsonUtils.OBJECT_MAPPER.writeValueAsBytes(nodeInfo))

      val mockZK = mock[ZooKeeperClient]
      mockZK.get().returns(mockZKClient)

      cluster.zk = mockZK
      cluster.previousZKSessionStillActive().must(be(true))

      mockZKClient.getSessionId.returns(102L)
      cluster.previousZKSessionStillActive().must(be(false))
    }

    @Test def `setState` {
      val nodeInfo = NodeInfo(NodeState.Draining.toString, 101L)
      val serialized = JsonUtils.OBJECT_MAPPER.writeValueAsBytes(nodeInfo)

      val mockZKClient = mock[ZooKeeper]
      mockZKClient.setData("/%s/nodes/%s".format(id, "testNode"), serialized, -1).returns(mock[Stat])
      mockZKClient.getSessionId.returns(101L)

      val mockZK = mock[ZooKeeperClient]
      mockZK.get().returns(mockZKClient)

      cluster.zk = mockZK
      cluster.setState(NodeState.Draining)

      verify.one(mockZKClient).setData("/%s/nodes/%s".format(id, "testNode"), serialized, -1)
      cluster.state.get().must(be(NodeState.Draining))
    }

    @Test def `zNode is Me` {
      val path = "/foo/bar"
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient

      mockZK.getData(equalTo(path), any[Boolean], any[Stat]).returns("testNode".getBytes)
      cluster.znodeIsMe(path).must(be(true))
      verify.one(mockZK).getData(equalTo(path), any[Boolean], any[Stat])

      mockZK.getData(equalTo(path), any[Boolean], any[Stat]).returns("SOME OTHER NODE".getBytes)
      cluster.znodeIsMe(path).must(be(false))
      verify.exactly(2)(mockZK).getData(equalTo(path), any[Boolean], any[Stat])
    }

    @Test def `rebalance invokes rebalance (only if not fresh)` {
      val policy = mock[BalancingPolicy]
      cluster.balancingPolicy = policy

      cluster.state.set(NodeState.Fresh)
      cluster.rebalance()
      verify.exactly(0)(policy).rebalance()

      cluster.state.set(NodeState.Started)
      cluster.rebalance()
      verify.one(policy).rebalance()
    }

    @Test def `shutdown work` {
      val policy = mock[BalancingPolicy]
      cluster.balancingPolicy = policy

      val work = "taco"
      val path = cluster.workUnitClaimPath(work)

      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient
      cluster.allWorkUnits = new HashMap[String, ObjectNode]

      cluster.myWorkUnits.add(work)
      cluster.myWorkUnits.contains(work).must(be(true))

      mockZK.getData(equalTo(path), any[Boolean], any[Stat]).answersWith((inv: InvocationOnMock) => {
        inv.getArguments()(2).asInstanceOf[Stat].setVersion(100)
        cluster.myNodeID.getBytes(Charsets.UTF_8)
      })
      cluster.shutdownWork(work, doLog = false)
      verify.one(mockZK).delete(path, 100)
      verify.one(policy).onShutdownWork(work)
      verify.one(mockClusterListener).shutdownWork(work)
      cluster.myWorkUnits.contains(work).must(be(false))

      // Then, test the case where we do not want the ZNode to be deleted.
      val (mockZK2, mockZKClient2) = getMockZK()
      mockZK2.getData(equalTo(path), any[Boolean], any[Stat]).returns("othernode".getBytes(Charsets.UTF_8))
      cluster.zk = mockZKClient2
      cluster.myWorkUnits.add(work)
      cluster.shutdownWork(work, doLog = false)
      verify.exactly(0)(mockZK2).delete(equalTo(path), any[Int])
      cluster.myWorkUnits.contains(work).must(be(false))
    }

    @Test def `claim work` {
      val policy = mock[BalancingPolicy]
      cluster.balancingPolicy = policy

      cluster.state.set(NodeState.Fresh)
      cluster.claimWork()
      verify.exactly(0)(policy).claimWork()

      cluster.state.set(NodeState.Started)
      cluster.connected.set(true)
      cluster.claimWork()
      verify.one(policy).claimWork()
    }

    @Test def `schedule rebalancing` {
      val pol = mock[BalancingPolicy]
      cluster.balancingPolicy = pol

      cluster.state.set(NodeState.Started)
      verify.exactly(0)(pol).rebalance()

      cluster.scheduleRebalancing()
      Thread.sleep(1200)
      verify.one(pol).rebalance()

      Thread.sleep(1000)
      verify.exactly(2)(pol).rebalance()

      cluster.autoRebalanceFuture.get.cancel(true)
      cluster.autoRebalanceFuture = None
    }

    @Test def `ensure clean startup` {
      val pol = mock[BalancingPolicy]
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient
      cluster.balancingPolicy = pol

      cluster.myWorkUnits.add("foo")
      cluster.claimedForHandoff.add("bar")
      cluster.workUnitsPeggedToMe.add("baz")
      cluster.state.set(NodeState.Draining)
      cluster.allWorkUnits = new HashMap[String, ObjectNode]

      mockZK.getData(equalTo(cluster.workUnitClaimPath("foo")), any[Boolean], any[Stat])
        .returns(cluster.myNodeID.getBytes(Charsets.UTF_8))

      cluster.ensureCleanStartup()
      verify.one(pol).shutdown()

      val future = cluster.autoRebalanceFuture
      (future.isEmpty || future.get.isCancelled).must(be(true))
      verify.one(mockClusterListener).shutdownWork("foo")
      verify.one(mockClusterListener).onLeave()

      cluster.myWorkUnits.isEmpty.must(be(true))
      cluster.claimedForHandoff.isEmpty.must(be(true))
      cluster.workUnitsPeggedToMe.isEmpty.must(be(true))
      cluster.state.get().must(be(NodeState.Fresh))
    }

    @Test def `complete shutdown` {
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient

      cluster.state.set(NodeState.Started)
      cluster.completeShutdown()
      verify.one(mockZKClient).close()
      verify.one(mockClusterListener).onLeave()
      cluster.state.get().must(be(NodeState.Shutdown))
    }

    @Test def `request handoff` {
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient

      val workUnit = "burrito"
      val path = "/%s/handoff-requests/%s".format(cluster.name, workUnit)

      cluster.requestHandoff(workUnit)
      verify.one(mockZK).create(path, "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

    @Test def `shutdown` {
      val pol = new CountBalancingPolicy(cluster, config)
      cluster.balancingPolicy = pol
      cluster.myWorkUnits.clear()
      cluster.allWorkUnits = new HashMap[String, ObjectNode]

      val (_, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient

      cluster.state.set(NodeState.Started)
      cluster.shutdown()

      // Must cancel the auto rebalance future
      val future = cluster.autoRebalanceFuture
      (future.isEmpty || future.get.isCancelled).must(be(true))

      // Ensure that 'completeShutdown' was called, which closes the ZK conn,
      // resets the cluster state, and calls the cluster listener's onLeave method.
      verify.one(mockZKClient).close()
      verify.one(mockClusterListener).onLeave()
      cluster.state.get.must(be(NodeState.Shutdown))
    }

    @Test def `force shutdown` {
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient
      val pol = mock[BalancingPolicy]
      cluster.balancingPolicy = pol
      cluster.myWorkUnits.add("foo")
      cluster.allWorkUnits = new HashMap[String, ObjectNode]

      mockZK.getData(equalTo(cluster.workUnitClaimPath("foo")), any[Boolean], any[Stat])
        .returns(cluster.myNodeID.getBytes(Charsets.UTF_8))
      cluster.forceShutdown()

      // Must cancel the auto rebalance future
      val future = cluster.autoRebalanceFuture
      (future.isEmpty || future.get.isCancelled).must(be(true))

      verify.one(pol).shutdown()
      cluster.myWorkUnits.isEmpty.must(be(true))
      verify.one(mockClusterListener).onLeave()
    }

    @Test def `join cluster` {
      val (mockZK, mockZKClient) = getMockZK()
      mockZK.getSessionId.returns(101L)
      cluster.zk = mockZKClient
      val path = "/%s/nodes/%s".format(id, cluster.myNodeID)
      val nodeInfo = JsonUtils.OBJECT_MAPPER.writeValueAsBytes(NodeInfo(NodeState.Fresh.toString, 101L))

      cluster.joinCluster()
      verify.one(mockZK).create(path, nodeInfo, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

    @Test def `register watchers` {
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient

      // Pretend that the paths exist for the ZooKeeperMaps we're creating
      mockZK.exists(any[String], any[Watcher]).returns(mock[Stat])

      cluster.registerWatchers()

      cluster.nodes.isInstanceOf[ZooKeeperMap[String]].must(be(true))
      cluster.allWorkUnits.isInstanceOf[ZooKeeperMap[ObjectNode]].must(be(true))
      cluster.workUnitMap.isInstanceOf[ZooKeeperMap[String]].must(be(true))

      // Not using soft handoff (TODO: assert ZKMap w/soft handoff on)
      cluster.handoffRequests.isInstanceOf[HashMap[String, String]].must(be(true))
      cluster.handoffResults.isInstanceOf[HashMap[String, String]].must(be(true))

      // TODO: Test loadMap isinstanceof zkmap with smart balancing on.
    }

    @Test def `verify integrity` {
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient
      cluster.allWorkUnits = new HashMap[String, ObjectNode]
      cluster.workUnitMap = new HashMap[String, String]

      val nonexistent = collection.mutable.Set("shut", "me", "down")
      val mine = collection.mutable.Set("foo", "dong")
      val noLongerMine = collection.mutable.Set("bar", "baz")
      val claimedForHandoff = collection.mutable.Set("taco")
      
      cluster.myWorkUnits.addAll(mine)
      cluster.myWorkUnits.addAll(nonexistent)
      cluster.myWorkUnits.addAll(noLongerMine)
      cluster.myWorkUnits.addAll(claimedForHandoff)

      nonexistent.foreach(el =>
        cluster.zk.get().getData(equalTo(cluster.workUnitClaimPath(el)), any[Boolean], any[Stat])
          .throws(new NoNodeException())
      )

      val workUnitMap = collection.mutable.Map(
        "foo" -> "testNode", "bar" -> "bar", "baz" -> "baz",
        "dong" -> "testNode", "taco" -> "bong")

      val peg = JsonUtils.OBJECT_MAPPER.createObjectNode()
      peg.put(id, "NOTTESTNODE")
      cluster.zk.get().getData(equalTo(cluster.workUnitClaimPath("dong")), any[Boolean], any[Stat])
        .returns("NOTTESTNODE".getBytes(Charsets.UTF_8))
      val allUnits = collection.mutable.Map[String, ObjectNode]()
      workUnitMap.keySet.foreach(workUnit => {
        if ("dong".equals(workUnit)) {
          allUnits.put(workUnit, peg)
        } else {
          allUnits.put(workUnit, JsonUtils.OBJECT_MAPPER.createObjectNode())
        }
      })

      cluster.allWorkUnits.putAll(allUnits)
      cluster.workUnitMap.putAll(workUnitMap)
      cluster.claimedForHandoff.addAll(claimedForHandoff)

      nonexistent.foreach(node =>
        mockZK.getData(equalTo(cluster.workUnitClaimPath(node)), any[Boolean], any[Stat]).throws(new NoNodeException())
      )

      mockZK.getData(equalTo(cluster.workUnitClaimPath("bar")), any[Boolean], any[Stat]).returns("testNode".getBytes)
      mockZK.getData(equalTo(cluster.workUnitClaimPath("baz")), any[Boolean], any[Stat]).returns("someoneElse".getBytes)

      cluster.verifyIntegrity()

      // Should shut down {shut, me, down} because they've been removed from the cluster.
      // Should leave {bar} active because it's marked as served by me in ZK despite the ZK
      // map not yet reflecting this node's claim of the organization.
      // Should shut down {baz} because it's now being served by someone else.
      // Should shut down {dong} because it has been pegged to someone else.
      // Should leave {foo} active because it's currently served by me.
      // Should leave {taco} active because I have claimed it for handoff.
      List("shut", "me", "down", "baz", "dong").foreach { workUnit =>
        cluster.myWorkUnits.contains(workUnit).must(be(false))
      }

      List("foo", "taco", "bar").foreach { workUnit =>
        cluster.myWorkUnits.contains(workUnit).must(be(true))
      }
    }

    @Test def `on connect after already started` {
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient
      cluster.state.set(NodeState.Started)

      // Ensure that previousZKSessionStillActive() returns true
      val nodeInfo = NodeInfo(NodeState.Started.toString, 101L)
      mockZK.getSessionId.returns(101L)
      mockZK.getData(equalTo("/%s/nodes/testNode".format(id)), any[Boolean], any[Stat]).
        returns(JsonUtils.OBJECT_MAPPER.writeValueAsBytes(nodeInfo))

      cluster.onConnect()

      // No attempts to create paths etc. should be made, and the method should
      // short-circuit / exit early. We can verify this by ensuring that the ZK
      // client was only touched twice.
      verify.exactly(2)(mockZKClient).get()
    }

    @Test def `on connect and started, but unclean shutdown` {
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient
      cluster.state.set(NodeState.Started)
      cluster.allWorkUnits = new HashMap[String, ObjectNode]

      // Ensure that previousZKSessionStillActive() returns false
      val nodeInfo = NodeInfo(NodeState.Started.toString, 102L)
      mockZK.getSessionId.returns(101L)
      mockZK.getData(equalTo("/%s/nodes/testNode".format(id)), any[Boolean], any[Stat]).
        returns(JsonUtils.OBJECT_MAPPER.writeValueAsBytes(nodeInfo))

      // Pretend that the paths exist for the ZooKeeperMaps we're creating
      mockZK.exists(any[String], any[Watcher]).returns(mock[Stat])

      // Ensure that on an unclean startup, the "ensureCleanStartup" method is
      // called, which clears out existing work units among other things.
      cluster.myWorkUnits.add("foo")
      cluster.zk.get().getData(equalTo(cluster.workUnitClaimPath("foo")), any[Boolean], any[Stat])
        .throws(new NoNodeException())
      cluster.onConnect()
      cluster.myWorkUnits.isEmpty.must(be(true))
    }

    @Test def `on connect - standard fresh launch` {
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient
      cluster.watchesRegistered.set(false)

      val policy = mock[BalancingPolicy]
      cluster.balancingPolicy = policy

      cluster.state.set(NodeState.Fresh)

      // Pretend that the paths exist for the ZooKeeperMaps we're creating
      mockZK.exists(any[String], any[Watcher]).returns(mock[Stat])

      cluster.onConnect()

      verify.one(mockClusterListener).onJoin(any)
      verify.one(policy).onConnect()
      cluster.state.get().must(be(NodeState.Started))
      cluster.watchesRegistered.set(true)
    }

    @Test def `connect` {
      val (mockZK, mockZKClient) = getMockZK()
      val policy = mock[BalancingPolicy]
      cluster.balancingPolicy = policy

      // Pretend that the paths exist for the ZooKeeperMaps we're creating
      mockZK.exists(any[String], any[Watcher]).returns(mock[Stat])

      cluster.connect(Some(mockZKClient))

      // Apply same verifications as onConnect, as all of these should be called.
      //verify.one(mockClusterListener).onJoin(any)
      //verify.one(policy).onConnect()
      //cluster.state.get().must(be(NodeState.Started))
      //cluster.watchesRegistered.set(true)
    }
  }

  @Test def `join` {
    val (mockZK, mockZKClient) = getMockZK()
    cluster.zk = mockZKClient

    val policy = mock[BalancingPolicy]
    cluster.balancingPolicy = policy

    // Should no-op if draining.
    cluster.setState(NodeState.Draining)
    cluster.join().must(be(NodeState.Draining.toString))
    verify.exactly(0)(mockZKClient).get()

    // Should no-op if started.
    cluster.setState(NodeState.Started)
    cluster.join().must(be(NodeState.Started.toString))
    verify.exactly(0)(mockZKClient).get()

    // Pretend that the paths exist for the ZooKeeperMaps we're creating
    mockZK.exists(any[String], any[Watcher]).returns(mock[Stat])

    cluster.setState(NodeState.Fresh)
    cluster.join().must(be(NodeState.Started.toString))

    // Apply same verifications as connect, as all of these should be called.
    verify.one(mockClusterListener).onJoin(any)
    verify.one(policy).onConnect()
    cluster.state.get().must(be(NodeState.Started))
    cluster.watchesRegistered.set(true)
  }


  @Test def `join after shutdown` {
    val (mockZK, mockZKClient) = getMockZK()
    cluster.zk = mockZKClient

    val policy = mock[BalancingPolicy]
    cluster.balancingPolicy = policy

   // Pretend that the paths exist for the ZooKeeperMaps we're creating
    mockZK.exists(any[String], any[Watcher]).returns(mock[Stat])

    cluster.setState(NodeState.Shutdown)
    cluster.join().must(be(NodeState.Started.toString))

    // Apply same verifications as connect, as all of these should be called.
    verify.one(mockClusterListener).onJoin(any)
    verify.one(policy).onConnect()
    cluster.state.get().must(be(NodeState.Started))
    cluster.watchesRegistered.set(true)
  }


  @Test def `cluster constructor` {
    val cluster = new Cluster("foo", mockClusterListener, config)
    cluster.name.must(be("foo"))
    cluster.listener.must(be(mockClusterListener))
  }


  @Test def `getOrElse String` {
    val foo = new HashMap[String, String]
    foo.put("foo", "bar")

    cluster.getOrElse(foo, "foo", "taco").must(be("bar"))
    cluster.getOrElse(foo, "bar", "taco").must(be("taco"))
  }

  @Test def `getOrElse Double` {
    val foo = new HashMap[String, Double]
    foo.put("foo", 0.01d)
    cluster.getOrElse(foo, "foo", 0.02d).must(be(0.01d))
    cluster.getOrElse(foo, "bar", 0.02d).must(be(0.02d))
  }

  def getMockZK() : (ZooKeeper, ZooKeeperClient) = {
    val mockZK = mock[ZooKeeper]
    val mockZKClient = mock[ZooKeeperClient]
    mockZKClient.get().returns(mockZK)
    (mockZK, mockZKClient)
  }

}

