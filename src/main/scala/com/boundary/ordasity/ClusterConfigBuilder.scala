package com.boundary.ordasity

import scala.reflect.BeanProperty
import java.net.InetAddress

class ClusterConfigBuilder {

  // Defaults
  @BeanProperty var hosts = ""
  @BeanProperty var enableAutoRebalance = true
  @BeanProperty var autoRebalanceInterval = 60
  @BeanProperty var drainTime = 60
  @BeanProperty var useSmartBalancing = false
  @BeanProperty var zkTimeout = 3000
  @BeanProperty var workUnitName = "work-units"
  @BeanProperty var workUnitShortName = "work"
  @BeanProperty var nodeId = InetAddress.getLocalHost.getHostName
  @BeanProperty var useSoftHandoff = false
  @BeanProperty var handoffShutdownDelay = 10
  

  def build() : ClusterConfig = {
    val config = new ClusterConfig()
    config
  }
  
}
