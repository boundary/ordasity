package com.boundary.ordasity

import com.yammer.metrics.scala.Meter
import com.twitter.common.zookeeper.ZooKeeperClient

abstract class Listener {
  def onJoin(client: ZooKeeperClient)
  def onLeave()
  def shutdownWork(workUnit: String)
}

abstract class SmartListener extends Listener {
  def startWork(workUnit: String, meter: Meter)
}

abstract class ClusterListener extends Listener {
  def startWork(workUnit: String)
}

