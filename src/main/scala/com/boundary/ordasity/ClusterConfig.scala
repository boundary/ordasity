//
// Copyright 2011-2012, Boundary
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package com.boundary.ordasity

import java.net.InetAddress
import scala.reflect.BeanProperty

class ClusterConfig {

  // Defaults
  @BeanProperty var hosts = ""
  @BeanProperty var enableAutoRebalance = true
  @BeanProperty var autoRebalanceInterval = 60
  @BeanProperty var drainTime = 60
  @BeanProperty var useSmartBalancing = false
  @BeanProperty var zkTimeout = 3000
  @BeanProperty var workUnitZkChRoot = ""
  @BeanProperty var workUnitName = "work-units"
  @BeanProperty var workUnitShortName = "work"
  @BeanProperty var nodeId = InetAddress.getLocalHost.getHostName
  @BeanProperty var useSoftHandoff = false
  @BeanProperty var handoffShutdownDelay = 10

}

object ClusterConfig {
  def builder() = new ClusterConfigBuilder(new ClusterConfig)
}

class ClusterConfigBuilder(config: ClusterConfig) {
  def setHosts(hosts: String) : ClusterConfigBuilder = {
    config.hosts = hosts
    this
  }

  def setWorkUnitZkChRoot(root: String): ClusterConfigBuilder ={
    config.workUnitZkChRoot = root
    this
  }

  def setEnableAutoRebalance(enableAutoRebalance: Boolean) : ClusterConfigBuilder = {
    config.enableAutoRebalance = enableAutoRebalance
    this
  }

  def setAutoRebalanceInterval(autoRebalanceInterval: Int) : ClusterConfigBuilder = {
    config.autoRebalanceInterval = autoRebalanceInterval
    this
  }

  def setZkTimeout(zkTimeout: Int) : ClusterConfigBuilder = {
    config.zkTimeout = zkTimeout
    this
  }

  def setUseSmartBalancing(useSmartBalancing: Boolean) : ClusterConfigBuilder = {
    config.useSmartBalancing = useSmartBalancing
    this
  }

  def setDrainTime(drainTime: Int) : ClusterConfigBuilder = {
    config.drainTime = drainTime
    this
  }

  def setWorkUnitName(workUnitName: String) : ClusterConfigBuilder = {
    config.workUnitName = workUnitName
    this
  }

  def setWorkUnitShortName(workUnitShortName: String) : ClusterConfigBuilder = {
    config.workUnitShortName = workUnitShortName
    this
  }

  def setNodeId(nodeId: String) : ClusterConfigBuilder = {
    config.nodeId = nodeId
    this
  }

  def setUseSoftHandoff(useSoftHandoff: Boolean) : ClusterConfigBuilder = {
    config.useSoftHandoff = useSoftHandoff
    this
  }

  def setHandoffShutdownDelay(handoffShutdownDelay: Int) : ClusterConfigBuilder = {
    config.handoffShutdownDelay = handoffShutdownDelay
    this
  }

  def build() : ClusterConfig = {
    config
  }
}
