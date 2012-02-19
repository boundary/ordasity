//
// Copyright 2011, Boundary
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

import java.nio.charset.Charset
import com.codahale.logula.Logging
import com.codahale.jerkson.Json
import com.google.common.base.Function

/**
 * Case class representing the state of a node and its ZooKeeper connection
 * ID. Used to differentiate nodes and connection states in the event that a
 * node is evicted and comes back up.
 */
case class NodeInfo(state: String, connectionID: Long)


/**
 * Enum representing the state of an Ordasity node.
 * One of: {Fresh, Started, Draining, Shutdown}
 */
object NodeState extends Enumeration {
  type NodeState = Value
  val Fresh, Started, Draining, Shutdown = Value
  
  def valueOf(s: String) : Option[NodeState.Value] = {
    try {
      withName(s) match {
        case e: Value => Some(e)
        case _ => None
      }
    } catch {
      case e: NoSuchElementException => None
    }
  }
}


/**
 * Utility method for converting an array of bytes to a NodeInfo object.
 */
class NodeInfoDeserializer extends Function[Array[Byte], NodeInfo] with Logging {
  def apply(bytes: Array[Byte]) : NodeInfo = {
    try {
      val data = new String(bytes, Charset.forName("UTF-8"))
      Json.parse[NodeInfo](data)
    } catch {
      case e: Exception =>
        val data = if (bytes == null) "" else new String(bytes)
        val parsedState = NodeState.valueOf(data).getOrElse(NodeState.Shutdown)
        val info = new NodeInfo(parsedState.toString, 0)
        log.warn("Saw node data in non-JSON format. Interpreting %s as: %s", data, info)
        info
    }
  }
}

/**
 * Utility method for converting an array of bytes to a String.
 */
class StringDeserializer extends Function[Array[Byte], String] {
  def apply(a: Array[Byte]) : String = {
    try {
      new String(a)
    } catch {
      case e: Exception => ""
    }
  }
}

/**
 * Utility method for converting an array of bytes to a Double.
 */
class DoubleDeserializer extends Function[Array[Byte], Double] {
  def apply(a: Array[Byte]) : Double = {
    try {
      new String(a).toDouble
    } catch {
      case e: Exception => 0d
    }
  }
}
