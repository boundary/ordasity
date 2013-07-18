package com.boundary.ordasity

class ClusterConfigWrapper {
  private[this] val config = new ClusterConfig()

  def setHosts(to: String) : ClusterConfig = {
    config.setHosts(to)
  }

  def getHosts() : String = {
    config.hosts
  }

  def setEnableAutoRebalance(to: Boolean) : ClusterConfig = {
    config.setAutoRebalance(to)
  }

  def getEnableAutoRebalance() : Boolean = {
    config.enableAutoRebalance
  }

  def setAutoRebalanceInterval(to: Int) : ClusterConfig = {
    config.setRebalanceInterval(to)
  }

  def getAutoRebalanceInterval() : Int = {
    config.autoRebalanceInterval
  }

  def setZKTimeout(to: Int) : ClusterConfig = {
    config.setZKTimeout(to)
  }

  def getZKTimeout() : Int = {
    config.zkTimeout
  }

  def setUseSmartBalancing(to: Boolean) : ClusterConfig = {
    config.useSmartBalancing(to)
  }

  def getUseSmartBalancing() : Boolean = {
    config.useSmartBalancing
  }

  def setDrainTime(to: Int) : ClusterConfig = {
    config.setDrainTime(to)
  }

  def getDrainTime() : Int = {
    config.drainTime
  }

  def setWorkUnitName(to: String) : ClusterConfig = {
    config.setWorkUnitName(to)
  }

  def getWorkUnitName() : String = {
    config.workUnitName
  }

  def setWorkUnitShortName(to: String) : ClusterConfig = {
    config.setWorkUnitShortName(to)
  }

  def getWorkUnitShortName() : String = {
    config.workUnitShortName
  }

  def setNodeId(to: String) : ClusterConfig = {
    config.setNodeId(to)
  }

  def getNodeId() : String = {
    config.nodeId
  }

  def setUseSoftHandoff(to: Boolean) : ClusterConfig = {
    config.setUseSoftHandoff(to)
  }

  def getUseSoftHandoff() : Boolean = {
    config.useSoftHandoff
  }

  def setHandoffShutdownDelay(to: Int) : ClusterConfig = {
    config.setHandoffShutdownDelay(to)
  }

  def getHandoffShutdownDelay() : Int = {
    config.handoffShutdownDelay
  }

}
