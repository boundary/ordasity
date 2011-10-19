package com.boundary.ordasity

import com.yammer.metrics.Meter
import com.twitter.zookeeper.ZooKeeperClient

class MyService {
  val config = new ClusterConfig("localhost:2181").
    setAutoRebalance(true).
    setRebalanceInterval(60).
    useSmartBalancing(true).
    setDrainTime(60).
    setZKTimeout(3)

  val cluster = new Cluster("ServiceName", listener, config)

  val listener = new SmartListener {
    def onJoin(client: ZooKeeperClient) = null // You are *in*, baby.

    def startWork(workUnit: String, meter: Meter) = null // Do yer thang, mark dat meter.

    def shutdownWork(workUnit: String) = null // Stop doin' that thang

    def onLeave() = null
  }

  cluster.join()
}
