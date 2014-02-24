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
import org.codehaus.jackson.annotate.JsonProperty

class ClusterConfig {

  // Defaults
  @JsonProperty var hosts = ""
  @JsonProperty var enableAutoRebalance = true
  @JsonProperty var autoRebalanceInterval = 60
  @JsonProperty var drainTime = 60
  @JsonProperty var useSmartBalancing = false
  @JsonProperty var zkTimeout = 3000
  @JsonProperty var workUnitName = "work-units"
  @JsonProperty var workUnitShortName = "work"
  @JsonProperty var nodeId = InetAddress.getLocalHost.getHostName
  @JsonProperty var useSoftHandoff = false
  @JsonProperty var handoffShutdownDelay = 10

  def setHosts(to: String) : ClusterConfig = {
    hosts = to
    this
  }

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

  def useSmartBalancing(to : Boolean) = setUseSmartBalancing(to)

  def setUseSmartBalancing(to: Boolean) : ClusterConfig = {
    useSmartBalancing = to
    this
  }

  def setDrainTime(to: Int) : ClusterConfig = {
    drainTime = to
    this
  }

  def setWorkUnitName(to: String) : ClusterConfig = {
    workUnitName = to
    this
  }

  def setWorkUnitShortName(to: String) : ClusterConfig = {
    workUnitShortName = to
    this
  }

  def setNodeId(to: String) : ClusterConfig = {
    nodeId = to
    this
  }

  def setUseSoftHandoff(to: Boolean) : ClusterConfig = {
    useSoftHandoff = to
    this
  }

  def setHandoffShutdownDelay(to: Int) : ClusterConfig = {
    handoffShutdownDelay = to
    this
  }

}
