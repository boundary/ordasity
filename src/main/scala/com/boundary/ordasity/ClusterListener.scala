package com.boundary.ordasity

import com.yammer.metrics.Meter
import com.twitter.common.zookeeper.ZooKeeperClient

trait Listener {
  def onJoin(client: ZooKeeperClient)
  def onLeave()
  def shutdownWork(workUnit: String)
}

trait SmartListener extends Listener {
  def startWork(workUnit: String, meter: Meter)
}

trait ClusterListener extends Listener {
  def startWork(workUnit: String)
}

