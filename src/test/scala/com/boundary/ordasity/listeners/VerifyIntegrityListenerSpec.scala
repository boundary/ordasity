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
import com.codahale.simplespec.Spec
import com.codahale.logula.Logging

import java.util.HashMap
import java.util.concurrent.atomic.AtomicBoolean
import org.cliffc.high_scale_lib.NonBlockingHashSet

import com.boundary.ordasity.{Claimer, Cluster, ClusterConfig}
import com.boundary.ordasity.balancing.MeteredBalancingPolicy

class VerifyIntegrityListenerSpec extends Spec with Logging {
  Logging.configure()

  val config = new ClusterConfig().
    setNodeId("testNode").
    setRebalanceInterval(1).
    setDrainTime(1).
    setHosts("no_existe:2181")

  class `Verify Integrity Listener` {

    @Test def `node changed` {
      val cluster = mock[Cluster]
      cluster.watchesRegistered.returns(new AtomicBoolean(true))
      cluster.initialized.returns(new AtomicBoolean(true))
      cluster.workUnitsPeggedToMe.returns(new NonBlockingHashSet[String])
      cluster.balancingPolicy.returns(new MeteredBalancingPolicy(cluster, config))
      cluster.myNodeID.returns("testNode")
      val claimer = new Claimer(cluster)
      claimer.start
      cluster.claimer.returns(claimer)

      val listener = new VerifyIntegrityListener(cluster, config)
      listener.nodeChanged("foo", "bar")

      verify.one(cluster).verifyIntegrity()
    }

    @Test def `node changed (pegged to me)` {
      val cluster = mock[Cluster]
      cluster.watchesRegistered.returns(new AtomicBoolean(true))
      cluster.initialized.returns(new AtomicBoolean(true))
      cluster.workUnitsPeggedToMe.returns(new NonBlockingHashSet[String])
      cluster.balancingPolicy.returns(new MeteredBalancingPolicy(cluster, config))

      cluster.myNodeID.returns("testNode")
      cluster.name.returns("foo")

      val workUnitMap = new HashMap[String, String]
      workUnitMap.put("foo", "{\"foo\": \"testNode\"}")
      cluster.allWorkUnits.returns(workUnitMap)

      val claimer = new Claimer(cluster)
      claimer.start
      cluster.claimer.returns(claimer)

      val listener = new VerifyIntegrityListener(cluster, config)
      listener.nodeChanged("foo", "bar")

      verify.one(cluster).claimWork()
      verify.one(cluster).verifyIntegrity()
    }

    @Test def `node removed` {
      val cluster = mock[Cluster]
      cluster.watchesRegistered.returns(new AtomicBoolean(true))
      cluster.initialized.returns(new AtomicBoolean(true))

      val claimer = new Claimer(cluster)
      claimer.start
      cluster.claimer.returns(claimer)

      val listener = new VerifyIntegrityListener(cluster, config)
      listener.nodeRemoved("foo")

      verify.one(cluster).claimWork()
      verify.one(cluster).verifyIntegrity()
    }

    @Test def `node changed - watches unregistered` {
      val cluster = mock[Cluster]
      cluster.watchesRegistered.returns(new AtomicBoolean(false))
      cluster.initialized.returns(new AtomicBoolean(false))

      val listener = new VerifyIntegrityListener(cluster, config)
      listener.nodeChanged("foo", "bar")

      verify.exactly(0)(cluster).claimWork()
      verify.exactly(0)(cluster).verifyIntegrity()
    }

    @Test def `node removed - watches unregistered` {
      val cluster = mock[Cluster]
      cluster.watchesRegistered.returns(new AtomicBoolean(false))
      cluster.initialized.returns(new AtomicBoolean(false))

      val listener = new VerifyIntegrityListener(cluster, config)
      listener.nodeRemoved("foo")

      verify.exactly(0)(cluster).claimWork()
      verify.exactly(0)(cluster).verifyIntegrity()
    }
  }
}
