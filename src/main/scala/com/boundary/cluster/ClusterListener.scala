package com.boundary.cluster

import com.yammer.metrics.Meter
import com.twitter.zookeeper.ZooKeeperClient

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

