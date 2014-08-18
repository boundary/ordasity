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

import com.google.common.base.Function
import com.boundary.logula.Logging
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory

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
      JsonUtils.OBJECT_MAPPER.readValue(bytes, classOf[NodeInfo])
    } catch {
      case e: Exception =>
        val data = if (bytes == null) "" else new String(bytes)
        val parsedState = NodeState.valueOf(data).getOrElse(NodeState.Shutdown)
        val info = new NodeInfo(parsedState.toString, 0)
        log.warn(e, "Saw node data in non-JSON format. Interpreting %s as: %s", data, info)
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

object JsonUtils {
  val OBJECT_MAPPER = new ObjectMapper()
  OBJECT_MAPPER.registerModule(new DefaultScalaModule)
}

class ObjectNodeDeserializer extends Function[Array[Byte], ObjectNode] {
  private[this] val LOGGER = LoggerFactory.getLogger(getClass)

  override def apply(input: Array[Byte]): ObjectNode = {
    if (input != null && input.length > 0) {
      try {
        return JsonUtils.OBJECT_MAPPER.readTree(input).asInstanceOf[ObjectNode]
      } catch {
        case e: Exception =>
          LOGGER.error("Failed to de-serialize ZNode", e)
      }
    }
    JsonUtils.OBJECT_MAPPER.createObjectNode()
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
