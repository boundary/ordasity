package com.boundary.cluster

import java.net.InetAddress

class ClusterConfig(val hosts: String) {

  // Defaults
  var enableAutoRebalance = true
  var autoRebalanceInterval = 60
  var drainTime = 60
  var useSmartBalancing = false
  var zkTimeout = 3000
  var workUnitName = "work-units"
  var workUnitShortName = "work"
  var nodeId = InetAddress.getLocalHost.getHostName
  var useSoftHandoff = false
  var handoffShutdownDelay = 10

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
