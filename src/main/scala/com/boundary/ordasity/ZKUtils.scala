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

import com.twitter.common.zookeeper.{ZooKeeperUtils, ZooKeeperClient}

import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import org.apache.zookeeper.{Watcher, CreateMode}
import org.apache.zookeeper.data.Stat
import org.slf4j.LoggerFactory

object ZKUtils {

  val log = LoggerFactory.getLogger(getClass)

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
        log.warn("No ZNode to delete for %s".format(path))
        false
      case e: Exception =>
        log.error("Unexpected error deleting ZK node %s".format(path), e)
        false
    }
  }

  /**
   * Attempts to atomically delete the ZNode with the specified path and value. Should be preferred over calling
   * delete() if the value is known.
   *
   * @param zk ZooKeeper client.
   * @param path Path to be deleted.
   * @param expectedValue The expected value of the ZNode at the specified path.
   * @return True if the path was deleted, false otherwise.
   */
  def deleteAtomic(zk: ZooKeeperClient, path: String, expectedValue: String) : Boolean = {
    val stat = new Stat()
    val value = getWithStat(zk, path, Some(stat))
    if (!expectedValue.equals(value)) {
      return false
    }
    try {
      zk.get().delete(path, stat.getVersion)
      true
    } catch {
      case e: Exception =>
        log.error("Failed to delete path %s with expected value %s".format(path, expectedValue), e)
        false
    }
  }

  def set(zk: ZooKeeperClient, path: String, data: String) : Boolean = {
    try {
      zk.get().setData(path, data.getBytes, -1)
      true
    } catch {
      case e: Exception =>
        log.error("Error setting %s to %s.".format(path, data), e)
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
    getWithStat(zk, path, None)
  }

  def getWithStat(zk: ZooKeeperClient, path: String, stat: Option[Stat]) : String = {
    try {
      val value = zk.get.getData(path, false, stat.orNull)
      new String(value)
    } catch {
      case e: NoNodeException =>
        null
      case e: Exception =>
        log.error("Error getting data for ZNode at path %s".format(path), e)
        null
    }
  }

  def exists(zk: ZooKeeperClient, path: String, watcher: Watcher = null) : Option[Stat] = {
    try {
      Option(zk.get().exists(path, watcher))
    } catch {
      case e: InterruptedException =>
        throw e
      case e: Exception =>
        log.error("Failed to get stat for ZNode at path %s".format(path))
        None
    }
  }

}
