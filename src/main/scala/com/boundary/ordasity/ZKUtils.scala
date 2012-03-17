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
import com.twitter.common.zookeeper.{ZooKeeperUtils, ZooKeeperClient}

import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import org.apache.zookeeper.{WatchedEvent, Watcher, KeeperException, CreateMode}
import org.apache.zookeeper.Watcher.Event.EventType

object ZKUtils extends Logging {

  def ensureOrdasityPaths(zk: ZooKeeperClient, name: String,  unit: String, unitShort: String) {
    val acl = Ids.OPEN_ACL_UNSAFE
    ZooKeeperUtils.ensurePath(zk, acl, "/%s/nodes".format(name))
    ZooKeeperUtils.ensurePath(zk, acl, "/%s".format(unit))
    ZooKeeperUtils.ensurePath(zk, acl, "/%s/meta/rebalance".format(name))
    ZooKeeperUtils.ensurePath(zk, acl, "/%s/meta/workload".format(name))
    ZooKeeperUtils.ensurePath(zk, acl, "/%s/claimed-%s".format(name, unitShort))
    ZooKeeperUtils.ensurePath(zk, acl, "/%s/handoff-requests".format(name))
    ZooKeeperUtils.ensurePath(zk, acl, "/%s/handoff-result".format(name))
  }

  def createEphemeral(zk: ZooKeeperClient, path: String, value: String = "") : Boolean = {
    val created = {
      try {
        zk.get().create(path, value.getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
        true
      } catch {
        case e: NodeExistsException => false
      }
    }

    created
  }

  def delete(zk: ZooKeeperClient, path: String) : Boolean = {
    try {
      zk.get().delete(path, -1)
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

  def set(zk: ZooKeeperClient, path: String, data: String) : Boolean = {
    try {
      zk.get().setData(path, data.getBytes, -1)
      true
    } catch {
      case e: Exception =>
        log.error(e, "Error setting %s to %s.", path, data)
        false
    }
  }


  def setOrCreate(zk: ZooKeeperClient, path: String,
                  data: String, mode: CreateMode = CreateMode.EPHEMERAL) {
    try {
      zk.get().setData(path, data.getBytes, -1)
    } catch {
      case e: NoNodeException =>
        zk.get().create(path, data.getBytes, Ids.OPEN_ACL_UNSAFE, mode)
    }
  }

  def get(zk: ZooKeeperClient, path: String) : String = {
    try {
      val value = zk.get.getData(path, false, null)
      new String(value)
    } catch {
      case e: NoNodeException =>
        null
      case e: Exception =>
        log.error(e, "Error getting data for ZNode at path %s", path)
        null
    }
  }

  /**
   * Watches a node. When the node's data is changed, onDataChanged will be called with the
   * new data value as a byte array. If the node is deleted, onDataChanged will be called with
   * None and will track the node's re-creation with an existence watch. Borrowed from the Twitter
   * scala-zookeeper-client by Alex Payne, released under Apache 2.0. THANKS AL3X!
   */
  def watchNode(zk: ZooKeeperClient, node : String, onDataChanged : Option[Array[Byte]] => Unit) {
    log.debug("Watching node %s", node)
    def updateData {
      try {
        onDataChanged(Some(zk.get.getData(node, dataGetter, null)))
      } catch {
        case e:KeeperException => {
          log.warn("Failed to read node %s: %s", node, e)
          deletedData
        }
      }
    }

    def deletedData {
      onDataChanged(None)
      if (zk.get.exists(node, dataGetter) != null) {
        // Node was re-created by the time we called zk.exist
        updateData
      }
    }

    def dataGetter = new Watcher {
      def process(event : WatchedEvent) {
        if (event.getType == EventType.NodeDataChanged || event.getType == EventType.NodeCreated) {
          updateData
        } else if (event.getType == EventType.NodeDeleted) {
          deletedData
        }
      }
    }
    updateData
  }

}
