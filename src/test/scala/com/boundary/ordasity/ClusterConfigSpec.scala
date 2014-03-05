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

import org.junit.Test
import java.net.InetAddress
import com.simple.simplespec.Spec

class ClusterConfigSpec extends Spec {

  class `Test Cluster Config` {
    @Test def `test defaults` {
      val conf = new ClusterConfig

      conf.hosts.must(be(""))
      conf.enableAutoRebalance.must(be(true))
      conf.autoRebalanceInterval.must(be(60))
      conf.drainTime.must(be(60))
      conf.useSmartBalancing.must(be(false))
      conf.zkTimeout.must(be(3000))
      conf.workUnitName.must(be("work-units"))
      conf.workUnitShortName.must(be("work"))
      conf.nodeId.must(be(InetAddress.getLocalHost.getHostName))
      conf.useSoftHandoff.must(be(false))
      conf.handoffShutdownDelay.must(be(10))
    }


    @Test def `test mutators` {
      ClusterConfig.builder().setHosts("foo").build().hosts.must(be("foo"))
      ClusterConfig.builder().setEnableAutoRebalance(false).build().enableAutoRebalance.must(be(false))
      ClusterConfig.builder().setAutoRebalanceInterval(10000).build().autoRebalanceInterval.must(be(10000))
      ClusterConfig.builder().setZkTimeout(333).build().zkTimeout.must(be(333))
      ClusterConfig.builder().setUseSmartBalancing(true).build().useSmartBalancing.must(be(true))
      ClusterConfig.builder().setDrainTime(100).build().drainTime.must(be(100))
      ClusterConfig.builder().setWorkUnitName("tacos").build().workUnitName.must(be("tacos"))
      ClusterConfig.builder().setWorkUnitShortName("taquitos").build().workUnitShortName.must(be("taquitos"))
      ClusterConfig.builder().setNodeId("skelter").build().nodeId.must(be("skelter"))
      ClusterConfig.builder().setUseSoftHandoff(true).build().useSoftHandoff.must(be(true))
      ClusterConfig.builder().setHandoffShutdownDelay(90).build().handoffShutdownDelay.must(be(90))
    }
  }
}
