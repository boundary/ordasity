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

import com.codahale.logula.Logging
import org.junit.Test
import java.net.InetAddress
import com.simple.simplespec.Spec

class ClusterConfigSpec extends Spec with Logging {
  Logging.configure()

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
      new ClusterConfig().setHosts("foo").hosts.must(be("foo"))
      new ClusterConfig().setAutoRebalance(false).enableAutoRebalance.must(be(false))
      new ClusterConfig().setRebalanceInterval(10000).autoRebalanceInterval.must(be(10000))
      new ClusterConfig().setZKTimeout(333).zkTimeout.must(be(333))
      new ClusterConfig().setUseSmartBalancing(true).useSmartBalancing.must(be(true))
      new ClusterConfig().setDrainTime(100).drainTime.must(be(100))
      new ClusterConfig().setWorkUnitName("tacos").workUnitName.must(be("tacos"))
      new ClusterConfig().setWorkUnitShortName("taquitos").workUnitShortName.must(be("taquitos"))
      new ClusterConfig().setNodeId("skelter").nodeId.must(be("skelter"))
      new ClusterConfig().setUseSoftHandoff(true).useSoftHandoff.must(be(true))
      new ClusterConfig().setHandoffShutdownDelay(90).handoffShutdownDelay.must(be(90))
    }
  }
}
