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

import org.apache.zookeeper.CreateMode
import com.twitter.zookeeper.ZooKeeperClient
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import com.codahale.logula.Logging

object ZKUtils extends Logging {

  def createEphemeral(zk: ZooKeeperClient, path: String, value: String = "") : Boolean = {
    val created = {
      try {
        zk.create(path, value.getBytes, CreateMode.EPHEMERAL)
        true
      } catch {
        case e: NodeExistsException => false
      }
    }

    created
  }

  def delete(zk: ZooKeeperClient, path: String) : Boolean = {
    try {
      zk.delete(path)
      true
    } catch {
      case e: NoNodeException =>
        log.warn("No ZNode to delete for %s", path)
        false
      case e: Exception =>
        log.error(e, "Unexpected error deleting ZK node %s", path)
        false
    }
  }

  def set(zk: ZooKeeperClient, path: String, data: String) {
    try {
      zk.set(path, data.getBytes)
      true
    } catch {
      case e: Exception =>
        log.error(e, "Error setting %s to %s.", path, data)
    }
  }


  def setOrCreate(zk: ZooKeeperClient, path: String, data: String) {
    try {
      zk.set(path, data.getBytes)
    } catch {
      case e: NoNodeException =>
        zk.create(path, data.getBytes, CreateMode.EPHEMERAL)
    }
  }

}
