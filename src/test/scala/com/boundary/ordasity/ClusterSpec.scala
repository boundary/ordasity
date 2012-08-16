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
import com.codahale.simplespec.Spec
import com.codahale.logula.Logging
import org.junit.Test
import java.util.{UUID, HashMap}
import com.codahale.jerkson.Json
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.ZooDefs.Ids
import com.twitter.common.zookeeper.{ZooKeeperMap, ZooKeeperClient}
import org.apache.zookeeper.{Watcher, CreateMode, ZooKeeper, WatchedEvent}
import org.apache.zookeeper.Watcher.Event.KeeperState

class ClusterSpec extends Spec with Logging {
  Logging.configure()

  val id = UUID.randomUUID().toString
  val config = new ClusterConfig().
    setNodeId("testNode").
    setRebalanceInterval(1).
    setDrainTime(1).
    setHosts("no_existe:2181")

  val mockClusterListener = mock[Listener]
  val cluster = new Cluster(id, mockClusterListener, config)

  class `Test Cluster` {

    @Test def `previous ZK session still active` {
      val nodeInfo = NodeInfo(NodeState.Started.toString, 101L)

      val mockZKClient = mock[ZooKeeper]
      mockZKClient.getSessionId.returns(101L)
      mockZKClient.getData("/%s/nodes/testNode".format(id), false, null).
        returns(Json.generate(nodeInfo).getBytes)

      val mockZK = mock[ZooKeeperClient]
      mockZK.get().returns(mockZKClient)

      cluster.zk = mockZK
      cluster.previousZKSessionStillActive().must(be(true))

      mockZKClient.getSessionId.returns(102L)
      cluster.previousZKSessionStillActive().must(be(false))
    }

    @Test def `setState` {
      val nodeInfo = NodeInfo(NodeState.Draining.toString, 101L)
      val serialized = Json.generate(nodeInfo).getBytes

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

      mockZK.getData(path, false, null).returns("testNode".getBytes)
      cluster.znodeIsMe(path).must(be(true))
      verify.one(mockZK).getData(path, false, null)

      mockZK.getData(path, false, null).returns("SOME OTHER NODE".getBytes)
      cluster.znodeIsMe(path).must(be(false))
      verify.exactly(2)(mockZK).getData(path, false, null)
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
      val path = "/%s/claimed-%s/%s".format(cluster.name, config.workUnitShortName, work)

      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient

      cluster.myWorkUnits.add(work)
      cluster.myWorkUnits.contains(work).must(be(true))

      // First, test the case where we request the ZNode be deleted.
      cluster.shutdownWork(work, doLog = false, deleteZNode = true)
      verify.one(mockZK).delete(path, -1)
      verify.one(policy).onShutdownWork(work)
      verify.one(mockClusterListener).shutdownWork(work)
      cluster.myWorkUnits.contains(work).must(be(false))

      // Then, test the case where we do not want the ZNode to be deleted.
      val (mockZK2, mockZKClient2) = getMockZK()
      cluster.zk = mockZKClient2
      cluster.myWorkUnits.add(work)
      cluster.shutdownWork(work, doLog = false, deleteZNode = false)
      verify.exactly(0)(mockZK2).delete(path, -1)
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

      log.info("Waiting one second for rebalance future to invoke.")
      cluster.scheduleRebalancing()
      Thread.sleep(1200)
      verify.one(pol).rebalance()

      log.info("Waiting one second for rebalance future to invoke again...")
      Thread.sleep(1000)
      verify.exactly(2)(pol).rebalance()

      cluster.autoRebalanceFuture.get.cancel(true)
      cluster.autoRebalanceFuture = None
    }

    @Test def `ensure clean startup` {
      val pol = mock[BalancingPolicy]
      cluster.balancingPolicy = pol

      cluster.myWorkUnits.add("foo")
      cluster.claimedForHandoff.add("bar")
      cluster.workUnitsPeggedToMe.add("baz")
      cluster.state.set(NodeState.Draining)

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

      val (mockZK, mockZKClient) = getMockZK()
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
      val pol = mock[BalancingPolicy]
      cluster.balancingPolicy = pol
      cluster.myWorkUnits.add("foo")
      
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
      val nodeInfo = Json.generate(NodeInfo(NodeState.Fresh.toString, 101L)).getBytes

      cluster.joinCluster()
      verify.one(mockZK).create(path, nodeInfo, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

    @Test def `register watchers` {
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient

      // Pretend that the paths exist for the ZooKeeperMaps we're creating
      mockZK.exists(any[String], any[Watcher]).returns(mock[Stat])

      cluster.registerWatchers()

      cluster.nodes.isInstanceOf[ZooKeeperMap[_]].must(be(true))
      cluster.allWorkUnits.isInstanceOf[ZooKeeperMap[_]].must(be(true))
      cluster.workUnitMap.isInstanceOf[ZooKeeperMap[_]].must(be(true))

      // Not using soft handoff (TODO: assert ZKMap w/soft handoff on)
      cluster.handoffRequests.isInstanceOf[HashMap[_, _]].must(be(true))
      cluster.handoffResults.isInstanceOf[HashMap[_, _]].must(be(true))

      // TODO: Test loadMap isinstanceof zkmap with smart balancing on.
    }

    @Test def `verify integrity` {
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient
      cluster.allWorkUnits = new HashMap[String, String]
      cluster.workUnitMap = new HashMap[String, String]

      val nonexistent = collection.mutable.Set("shut", "me", "down")
      val mine = collection.mutable.Set("foo", "dong")
      val noLongerMine = collection.mutable.Set("bar", "baz")
      val claimedForHandoff = collection.mutable.Set("taco")
      
      cluster.myWorkUnits.addAll(mine)
      cluster.myWorkUnits.addAll(nonexistent)
      cluster.myWorkUnits.addAll(noLongerMine)
      cluster.myWorkUnits.addAll(claimedForHandoff)

      val workUnitMap = collection.mutable.Map(
        "foo" -> "testNode", "bar" -> "bar", "baz" -> "baz",
        "dong" -> "testNode", "taco" -> "bong")

      val peg = Json.generate(Map(id -> "NOTTESTNODE"))
      val allUnits = collection.mutable.Map(
        "foo" -> "", "bar" -> "", "baz" -> "", "dong" -> peg, "taco" -> "")

      cluster.allWorkUnits.putAll(allUnits)
      cluster.workUnitMap.putAll(workUnitMap)
      cluster.claimedForHandoff.addAll(claimedForHandoff)

      mockZK.getData("/%s/claimed-work/bar".format(id), false, null).returns("testNode".getBytes)
      mockZK.getData("/%s/claimed-work/baz".format(id), false, null).returns("someoneElse".getBytes)

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
      mockZK.getData("/%s/nodes/testNode".format(id), false, null).
        returns(Json.generate(nodeInfo).getBytes)

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

      // Ensure that previousZKSessionStillActive() returns false
      val nodeInfo = NodeInfo(NodeState.Started.toString, 102L)
      mockZK.getSessionId.returns(101L)
      mockZK.getData("/%s/nodes/testNode".format(id), false, null).
        returns(Json.generate(nodeInfo).getBytes)

      // Pretend that the paths exist for the ZooKeeperMaps we're creating
      mockZK.exists(any[String], any[Watcher]).returns(mock[Stat])

      // Ensure that on an unclean startup, the "ensureCleanStartup" method is
      // called, which clears out existing work units among other things.
      cluster.myWorkUnits.add("foo")
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
    }

    @Test def `connect` {
      val (mockZK, mockZKClient) = getMockZK()
      val policy = mock[BalancingPolicy]
      cluster.balancingPolicy = policy

      // Pretend that we get a SyncConnected event during our register call (synchronously)
      mockZKClient.register(any).answersWith { _.getArguments match {
        case Array(watcher: Watcher) => watcher.process(new WatchedEvent(null, KeeperState.SyncConnected, null))
      }}

      // Pretend that the paths exist for the ZooKeeperMaps we're creating
      mockZK.exists(any[String], any[Watcher]).returns(mock[Stat])

      cluster.connect(Some(mockZKClient))

      // Apply same verifications as onConnect, as all of these should be called.
      verify.one(mockClusterListener).onJoin(any)
      verify.one(policy).onConnect()
      cluster.state.get().must(be(NodeState.Started))
    }

    class `join` {
      val (mockZK, mockZKClient) = getMockZK()
      cluster.zk = mockZKClient

      val policy = mock[BalancingPolicy]
      cluster.balancingPolicy = policy

      // Pretend that we get a SyncConnected event during our register call (synchronously)
      mockZKClient.register(any).answersWith { _.getArguments match {
        case Array(watcher: Watcher) => watcher.process(new WatchedEvent(null, KeeperState.SyncConnected, null))
      }}

      // Pretend that the paths exist for any ZooKeeperMaps we might create
      mockZK.exists(any[String], any[Watcher]).returns(mock[Stat])

      @Test def `when draining` {
        cluster.setState(NodeState.Draining)
        cluster.join(Some(mockZKClient)).must(be(NodeState.Draining.toString))

        // Should no-op if draining.
        verify.no(mockClusterListener).onJoin(any)
        verify.no(policy).onConnect()
        cluster.state.get().must(be(NodeState.Draining))
        cluster.watchesRegistered.set(false)
      }

      @Test def `after started` {
        cluster.setState(NodeState.Started)
        cluster.join(Some(mockZKClient)).must(be(NodeState.Started.toString))

        // Should no-op if already started.
        verify.no(mockClusterListener).onJoin(any)
        verify.no(policy).onConnect()
        cluster.state.get().must(be(NodeState.Started))
        cluster.watchesRegistered.set(false)
      }

      @Test def `when fresh` {
        cluster.setState(NodeState.Fresh)
        cluster.join(Some(mockZKClient)).must(be(NodeState.Started.toString))

        // Apply same verifications as connect, as all of these should be called.
        verify.one(mockClusterListener).onJoin(any)
        verify.one(policy).onConnect()
        cluster.state.get().must(be(NodeState.Started))
      }

      @Test def `after shutdown` {
        cluster.setState(NodeState.Shutdown)
        cluster.join(Some(mockZKClient)).must(be(NodeState.Shutdown.toString))

        // Should no-op if shutdown.
        verify.no(mockClusterListener).onJoin(any)
        verify.no(policy).onConnect()
        cluster.state.get().must(be(NodeState.Shutdown))
      }
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
}

