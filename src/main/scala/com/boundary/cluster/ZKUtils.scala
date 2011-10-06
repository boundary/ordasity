package com.boundary.cluster

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

  def setOrCreate(zk: ZooKeeperClient, path: String, data: String) {
    try {
      zk.set(path, data.getBytes)
    } catch {
      case e: NoNodeException =>
        zk.create(path, data.getBytes, CreateMode.PERSISTENT)
    }
  }

}
