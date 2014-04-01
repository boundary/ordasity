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

import org.junit.Test
import java.util.concurrent.atomic.AtomicBoolean
import com.boundary.ordasity.{NodeState, NodeInfo, Cluster, ClusterConfig, Claimer}
import com.simple.simplespec.Spec

class ClusterNodesChangedListenerSpec extends Spec {

  val config = ClusterConfig.builder().
    setNodeId("testNode").
    setAutoRebalanceInterval(1).
    setDrainTime(1).
    setHosts("no_existe:2181").build()

  class `Cluster Nodes Changed Listener` {

    @Test def `node changed` {
      val cluster = mock[Cluster]
      cluster.watchesRegistered.returns(new AtomicBoolean(true))
      cluster.initialized.returns(new AtomicBoolean(true))

      val claimer = mock[Claimer]
      claimer.start()
      cluster.claimer.returns(claimer)
      claimer.requestClaim().answersWith(invocation => {
        cluster.claimWork()
        true
      })

      val listener = new ClusterNodesChangedListener(cluster)
      listener.nodeChanged("foo", NodeInfo(NodeState.Started.toString, 0L))

      verify.one(cluster).claimWork()
      verify.one(cluster).verifyIntegrity()
    }

    @Test def `node removed` {
      val cluster = mock[Cluster]
      cluster.watchesRegistered.returns(new AtomicBoolean(true))
      cluster.initialized.returns(new AtomicBoolean(true))

      val claimer = mock[Claimer]
      cluster.claimer.returns(claimer)
      claimer.requestClaim().answersWith(invocation => {
        cluster.claimWork()
        true
      })

      val listener = new ClusterNodesChangedListener(cluster)
      listener.nodeRemoved("foo")

      verify.one(cluster).claimWork()
      verify.one(cluster).verifyIntegrity()
    }

    @Test def `node changed - watches unregistered` {
      val cluster = mock[Cluster]
      cluster.watchesRegistered.returns(new AtomicBoolean(false))
      cluster.initialized.returns(new AtomicBoolean(false))

      val listener = new ClusterNodesChangedListener(cluster)
      listener.nodeChanged("foo", NodeInfo(NodeState.Started.toString, 0L))

      verify.exactly(0)(cluster).claimWork()
      verify.exactly(0)(cluster).verifyIntegrity()
    }

    @Test def `node removed - watches unregistered` {
      val cluster = mock[Cluster]
      cluster.watchesRegistered.returns(new AtomicBoolean(false))
      cluster.initialized.returns(new AtomicBoolean(false))

      val listener = new ClusterNodesChangedListener(cluster)
      listener.nodeRemoved("foo")

      verify.exactly(0)(cluster).claimWork()
      verify.exactly(0)(cluster).verifyIntegrity()
    }
  }
}
