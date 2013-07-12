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

import com.codahale.simplespec.Spec
import com.codahale.logula.Logging
import org.junit.Test
import org.cliffc.high_scale_lib.NonBlockingHashSet
import java.util.{UUID, HashMap}
import com.twitter.common.zookeeper.ZooKeeperClient
import com.boundary.ordasity._
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean}
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{WatchedEvent, Watcher, CreateMode, ZooKeeper}
import org.mockito.{ArgumentCaptor, Mockito}
import java.util.concurrent.ScheduledThreadPoolExecutor
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}

class HandoffResultsListenerSpec extends Spec with Logging {
  Logging.configure()

  val config = new ClusterConfig().
    setNodeId("testNode").
    setRebalanceInterval(1).
    setDrainTime(1).
    setHosts("no_existe:2181").
    setHandoffShutdownDelay(1)

  class `Handoff Results Listener` {

    @Test def `test 'i requested handoff'` {
      val cluster = new Cluster(UUID.randomUUID().toString, null, config)
      val listener = new HandoffResultsListener(cluster, config)

      cluster.handoffResults = new HashMap[String, String]
      cluster.handoffResults.put("workUnit", "otherNode")
      cluster.handoffResults.put("myWorkUnit", "testNode")
      cluster.handoffResults.put("somethingElse", "somewhereElse")

      cluster.myWorkUnits.add("workUnit")
      cluster.myWorkUnits.add("myWorkUnit")

      listener.iRequestedHandoff("workUnit").must(be(true))
      listener.iRequestedHandoff("myWorkUnit").must(be(false))
      listener.iRequestedHandoff("somethingElse").must(be(false))
      listener.iRequestedHandoff("nothing").must(be(false))
    }

    @Test def `test shutdown after handoff` {
      val cluster = mock[Cluster]
      val workUnit = "workUnit"

      val handoffResults = new HashMap[String, String]
      handoffResults.put(workUnit, "otherNode")

      val myWorkUnits = new NonBlockingHashSet[String]
      myWorkUnits.add(workUnit)

      cluster.handoffResults.returns(handoffResults)
      cluster.myWorkUnits.returns(myWorkUnits)

      cluster.state.returns(new AtomicReference(NodeState.Started))

      val listener = new HandoffResultsListener(cluster, config)
      listener.shutdownAfterHandoff(workUnit).run()

      verify.one(cluster).shutdownWork(workUnit, false, true)
      verify.no(cluster).shutdown()
    }

    @Test def `test cluster-wide shutdown after finishing all handoff` {
      val cluster = mock[Cluster]
      val workUnit = "workUnit"

      val handoffResults = new HashMap[String, String]
      handoffResults.put(workUnit, "otherNode")

      val myWorkUnits = new NonBlockingHashSet[String]
      myWorkUnits.add(workUnit)

      cluster.handoffResults.returns(handoffResults)
      cluster.myWorkUnits.returns(myWorkUnits)

      cluster.state.returns(new AtomicReference(NodeState.Draining))

      val listener = new HandoffResultsListener(cluster, config)
      listener.shutdownAfterHandoff(workUnit).run()

      // First, verify that we don't trigger full shutdown with a work unit remaining.
      verify.one(cluster).shutdownWork(workUnit, false, true)
      verify.no(cluster).shutdown()

      myWorkUnits.clear()

      // Then, verify that we do trigger shutdown once the work unit set is empty.
      listener.shutdownAfterHandoff(workUnit).run()
      verify.exactly(2)(cluster).shutdownWork(workUnit, false, true)
      verify.one(cluster).shutdown()
    }

    @Test def `test finish handoff` {
      val cluster = new Cluster(UUID.randomUUID().toString, null, config)
      val listener = new HandoffResultsListener(cluster, config)
      val workUnit = "workUnit"

      val mockZK = mock[ZooKeeper]
      val mockZKClient = mock[ZooKeeperClient]
      mockZKClient.get().returns(mockZK)
      cluster.zk = mockZKClient

      cluster.claimedForHandoff.add(workUnit)
      cluster.workUnitMap = new HashMap[String, String]
      cluster.workUnitMap.put("workUnit", "somewhereElse")

      val path = "/%s/claimed-%s/%s".format(cluster.name, config.workUnitShortName, workUnit)
      mockZK.create(path, cluster.myNodeID.getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL).returns("")
      mockZK.getData(path, false, null).returns("otherNode".getBytes)

      val captureWatchter = ArgumentCaptor.forClass(classOf[Watcher])
      Mockito.when(mockZK.exists(equalTo(path), captureWatchter.capture())).thenReturn(new Stat())

      listener.finishHandoff(workUnit)
      // Callback on the watcher
      captureWatchter.getValue.process(new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, path))
      cluster.claimedForHandoff.contains(workUnit).must(be(false))
    }

    @Test def `test finish handoff when exists is null` {
      val cluster = new Cluster(UUID.randomUUID().toString, null, config)
      val listener = new HandoffResultsListener(cluster, config)
      val workUnit = "workUnit"

      val mockZK = mock[ZooKeeper]
      val mockZKClient = mock[ZooKeeperClient]
      mockZKClient.get().returns(mockZK)
      cluster.zk = mockZKClient

      cluster.claimedForHandoff.add(workUnit)
      cluster.workUnitMap = new HashMap[String, String]
      cluster.workUnitMap.put("workUnit", "somewhereElse")

      val path = "/%s/claimed-%s/%s".format(cluster.name, config.workUnitShortName, workUnit)
      mockZK.exists(equalTo(path), any[Watcher]).returns(null)
      mockZK.create(path, cluster.myNodeID.getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

      listener.finishHandoff(workUnit)

      cluster.claimedForHandoff.contains(workUnit).must(be(false))
    }

    // TODO: Expand the scope of this test.
    // The big kahuna for 'i accepted handoff'
    @Test def `test apply for accepting handoff` {
      val workUnit = "workUnit"
      val cluster = new Cluster(UUID.randomUUID().toString, null, config)
      val listener = new HandoffResultsListener(cluster, config)

      val mockZK = mock[ZooKeeper]
      val mockZKClient = mock[ZooKeeperClient]
      mockZKClient.get().returns(mockZK)
      cluster.zk = mockZKClient

      val path = "/%s/claimed-%s/%s".format(cluster.name, config.workUnitShortName, workUnit)
      mockZK.getData(path, false, null).returns("otherNode".getBytes)

      Mockito.when(mockZK.create(path, cluster.myNodeID.getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)).
        thenReturn("")

      val captureWatchter = ArgumentCaptor.forClass(classOf[Watcher])
      Mockito.when(mockZK.exists(equalTo(path), captureWatchter.capture())).thenReturn(new Stat())

      cluster.watchesRegistered.set(true)
      cluster.initialized.set(true)
      cluster.handoffResults = new HashMap[String, String]
      cluster.workUnitMap = new HashMap[String, String]
      cluster.handoffResults.put(workUnit, "testNode")
      cluster.myWorkUnits.add(workUnit)
      cluster.claimedForHandoff.add(workUnit)
      cluster.handoffResultsListener.finishHandoff(workUnit)
      // Callback on the watcher
      captureWatchter.getValue.process(new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, path))

      cluster.workUnitMap = new HashMap[String, String]
      cluster.workUnitMap.put(workUnit, "somewhereElse")

      listener.apply(workUnit)

      cluster.claimedForHandoff.contains(workUnit).must(be(false))
    }

    /**
     * The big kahuna for 'i requested handoff'. This one's kinda heavy on the mocks.
     * State: I have a work unit in my active set, and handoffResults says that another
     * node has accepted handoff.
     */
    @Test def `test apply for requesting handoff` {
      val workUnit = "workUnit"
      val cluster = mock[Cluster]

      val handoffResults = new HashMap[String, String]
      handoffResults.put(workUnit, "otherNode")

      val myWorkUnits = new NonBlockingHashSet[String]
      myWorkUnits.add(workUnit)

      val claimedForHandoff = new NonBlockingHashSet[String]
      cluster.claimedForHandoff.returns(claimedForHandoff)

      // Mocks
      val mockZK = mock[ZooKeeper]
      val mockZKClient = mock[ZooKeeperClient]
      mockZKClient.get().returns(mockZK)
      cluster.zk.returns(mockZKClient)
      cluster.pool.returns(new AtomicReference[ScheduledThreadPoolExecutor](new ScheduledThreadPoolExecutor(1)))

      // More mocks.
      cluster.handoffResults.returns(handoffResults)
      cluster.myWorkUnits.returns(myWorkUnits)
      cluster.isMe("otherNode").returns(false)
      cluster.getOrElse(handoffResults, workUnit, "").returns("otherNode")
      cluster.watchesRegistered.returns(new AtomicBoolean(true))
      cluster.initialized.returns(new AtomicBoolean(true))
      cluster.state.returns(new AtomicReference(NodeState.Started))

      // Assert that the listener behaves correctly when called, given the above state.
      val listener = new HandoffResultsListener(cluster, config)
      listener.iRequestedHandoff(workUnit).must(be(true))
      listener.apply(workUnit)

      verify.one(mockZK).delete("/%s/handoff-requests/%s".format(cluster.name, workUnit), -1)

      Thread.sleep((config.handoffShutdownDelay * 1000) + 100)

      verify.one(cluster).shutdownWork(workUnit, false, true)
      verify.no(cluster).shutdown()
    }
  }
}































