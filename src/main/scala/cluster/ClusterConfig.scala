package cluster

class ClusterConfig(val hosts: String) {

  // Defaults
  var enableAutoRebalance = true
  var autoRebalanceInterval = 60
  var drainTime = 60
  var useSmartBalancing = false
  var zkTimeout = 3000
  var multiTenant = false

  def setAutoRebalance(to: Boolean) : ClusterConfig = {
    enableAutoRebalance = to
    this
  }

  def setRebalanceInterval(to: Int) : ClusterConfig = {
    autoRebalanceInterval = to
    this
  }

  def setZKTimeout(to: Int) : ClusterConfig = {
    zkTimeout = to
    this
  }

  def useSmartBalancing(to: Boolean) : ClusterConfig = {
    useSmartBalancing = to
    this
  }

   def setDrainTime(to: Int) : ClusterConfig = {
    drainTime = to
    this
  }

  def setMultitenant(to: Boolean) : ClusterConfig = {
    multiTenant = to
    this
  }
}
