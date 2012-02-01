package com.boundary.ordasity

import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException.{NoNodeException, NodeExistsException}
import com.codahale.logula.Logging
import com.twitter.common.zookeeper.ZooKeeperClient
import org.apache.zookeeper.ZooDefs.Ids

object ZKUtils extends Logging {

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

  def set(zk: ZooKeeperClient, path: String, data: String) {
    try {
      zk.get().setData(path, data.getBytes, -1)
      true
    } catch {
      case e: Exception =>
        log.error(e, "Error setting %s to %s.", path, data)
    }
  }


  def setOrCreate(zk: ZooKeeperClient, path: String, data: String) {
    try {
      zk.get().setData(path, data.getBytes, -1)
    } catch {
      case e: NoNodeException =>
        zk.get().create(path, data.getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
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
}
