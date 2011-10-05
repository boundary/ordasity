package cluster

import com.yammer.metrics.Meter

trait Listener {
  def shutdownWork(workUnit: String)
}

trait SmartListener extends Listener {
  def startWork(workUnit: String, meter: Meter)
}

trait ClusterListener extends Listener {
  def startWork(workUnit: String)
}

