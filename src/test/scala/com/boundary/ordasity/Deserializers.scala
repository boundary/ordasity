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
import com.codahale.jerkson.Json
import com.simple.simplespec.Spec

class DeserializersSpec extends Spec {

  class `Test Deserializers` {

    @Test def `nodeState` {
      NodeState.valueOf("Fresh").must(be(Some(NodeState.Fresh)))
      NodeState.valueOf("Started").must(be(Some(NodeState.Started)))
      NodeState.valueOf("Draining").must(be(Some(NodeState.Draining)))
      NodeState.valueOf("Shutdown").must(be(Some(NodeState.Shutdown)))
      NodeState.valueOf("taco").must(be(None))
    }

    @Test def `nodeinfo case class` {
      val info = NodeInfo("foo", 101L)
      info.state.must(be("foo"))
      info.connectionID.must(be(101L))
    }

    @Test def `node info deserializer` {
      val deser = new NodeInfoDeserializer

      val valid = NodeInfo("foo", 101L)
      val bytes = Json.generate(valid).getBytes

      deser.apply(bytes).must(be(valid))
      deser.apply(null).must(be(NodeInfo(NodeState.Shutdown.toString, 0)))
    }


    @Test def `string deserializer` {
      val deser = new StringDeserializer
      deser.apply("foo".getBytes).must(be("foo"))
      deser.apply(null).must(be(""))
    }

    @Test def `double deserializer` {
      val deser = new DoubleDeserializer
      deser.apply("0.151".getBytes).must(be(0.151))
      deser.apply(null).must(be(0d))
    }
  }
}
