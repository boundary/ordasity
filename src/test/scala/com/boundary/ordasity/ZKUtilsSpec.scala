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
import com.twitter.common.zookeeper.ZooKeeperClient
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, ZooKeeper}
import org.apache.zookeeper.KeeperException.NoNodeException
import com.simple.simplespec.Spec
import org.apache.zookeeper.data.Stat

class ZKUtilsSpec extends Spec {

  class `Test ZK Utils` {

    @Test def `test ensure ordasity paths` {
      val (mockZK, mockZKClient) = getMockZK()
      val clusterName = "foo"
      val unitName = "organizations"
      val unitShortName = "orgs"
      val roots : List[String] = List("", "/ROOT")

      for (root <- roots) {
        val config = ClusterConfig.builder()
          .setWorkUnitName(unitName)
          .setWorkUnitShortName(unitShortName)
          .setWorkUnitZkChRoot(root)
          .build()

        ZKUtils.ensureOrdasityPaths(mockZKClient, clusterName, config)

        val paths = List(
          "/%s".format(clusterName),
          "/%s/nodes".format(clusterName),
          "%s/%s".format(root, unitName),
          "/%s/meta/rebalance".format(clusterName),
          "/%s/claimed-%s".format(clusterName, unitShortName),
          "/%s/handoff-requests".format(clusterName),
          "/%s/handoff-result".format(clusterName)
        )

        paths.foreach(path =>
          verify.atLeastOne(mockZK).create(path, null,
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        )
      }
    }

    @Test def `test create ephemeral node` {
      val (mockZK, mockZKClient) = getMockZK()
      val path = "/foo"
      val data = "data"

      ZKUtils.createEphemeral(mockZKClient, path, data).must(be(true))
      verify.atLeastOne(mockZK).create(path, data.getBytes,
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

    @Test def `delete znode` {
      val (mockZK, mockZKClient) = getMockZK()
      val path = "/delete_me"
      ZKUtils.delete(mockZKClient, path).must(be(true))
      verify.atLeastOne(mockZK).delete(path, -1)
    }

    @Test def `set znode to value` {
      val (mockZK, mockZKClient) = getMockZK()
      val path = "/set_me"
      val data = "to this"
      ZKUtils.set(mockZKClient, path, data).must(be(true))
      verify.atLeastOne(mockZK).setData(path, data.getBytes, -1)
    }

    @Test def `set or create` {
      val (mockZK, mockZKClient) = getMockZK()
      val path = "/set_me"
      val data = "to this"
      ZKUtils.setOrCreate(mockZKClient, path, data).must(be(true))
      verify.atLeastOne(mockZK).setData(path, data.getBytes, -1)
    }

    @Test def `set or *create*` {
      val (mockZK, mockZKClient) = getMockZK()
      val path = "/set_me"
      val data = "to this"

      mockZK.setData(path, data.getBytes, -1).throws(new NoNodeException())

      ZKUtils.setOrCreate(mockZKClient, path, data).must(be(true))
      verify.atLeastOne(mockZK).setData(path, data.getBytes, -1)
      verify.atLeastOne(mockZK).create(path, data.getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    }

    @Test def `test get` {
      val (mockZK, mockZKClient) = getMockZK()
      val path = "/foo"
      val data = "ohai"
      mockZK.getData(equalTo(path), any[Boolean], any[Stat]).returns(data.getBytes)

      ZKUtils.get(mockZKClient, path).must(be(data))
    }


    def getMockZK() : (ZooKeeper, ZooKeeperClient) = {
      val mockZK = mock[ZooKeeper]
      val mockZKClient = mock[ZooKeeperClient]
      mockZKClient.get().returns(mockZK)
      (mockZK, mockZKClient)
    }
  }
}
