# Ordasity API Documentation

## Table of Contents.
1. Overview
2. Cluster
3. ClusterConfig
4. ClusterListener and SmartListener
5. JMX

## Overview

Ordasity's public API is small, consisting of the "Cluster" class, "ClusterConfig" (its configuration), and a listener ("ClusterListener" or "SmartListener"). The library also exposes runtime metrics / instrumentation and a management interface via JMX.

---

## Cluster
#### Class Name: com.boundary.ordasity.Cluster

The "Cluster" class is the main point of interaction with Ordasity. It is used to initialize, join, and exit a cluster of nodes.

To initialize a new cluster, use its constructor:

    new Cluster(name: String, listener: Listener, config: ClusterConfig)
    
– The "Name" field denotes the name of the clustered service you're launching, as it should appear in Zookeeper.

– The Listener is either a ClusterListener or SmartListener (see #4), which directs your application to either start or shut down work.

– Finally, the ClusterConfig is a simple configuration class which defines the options and behavior of your cluster.

The Cluster class exposes two public methods intended to be used as the public API:

#### **join()**
Calling *cluster.join()* initializes the node's connection to Zookeeper, joins the cluster, claims work based on the policy specified, and begins operation.

#### **shutdown()**
Calling *cluster.shutdown() drains all work claimed by this node over the time period provided in the ClusterConfig (default: 60 seconds), prevents it from claiming new work, and exits the cluster.

---

## ClusterConfig
#### Class Name: com.boundary.ordasity.ClusterConfig

ClusterConfig defines your node's configuration. It is important that each node in your cluster be launched with the same configuration; behavior is otherwise undefined and could result in work units not being claimed.

A ClusterConfig is initialized by calling its constructor, which takes a Zookeeper connection string:

    new ClusterConfig("zookeeper-0:2181,zookeeper-1:2181,zookeeper-2:2181")

ClusterConfig uses a builder pattern (with defaults), allowing you to specify a configuration like so:

    val config = new ClusterConfig("localhost:2181").
      setAutoRebalance(true).
      setRebalanceInterval(60).
      useSmartBalancing(true).
      setDrainTime(60).
      setZKTimeout(3000)

ClusterConfig exposes 10 configuration options. Here are these options and their defaults:

#### enableAutoRebalance
*Default:* true

*Setter method:* setAutoRebalance(to: Boolean)

*Description*: The "enableAutoRebalance" parameter determines whether or not Ordasity should schedule periodic rebalancing automatically. If enabled, this will occur at the autoRebalanceInterval (see below).

#### autoRebalanceInterval
*Default:* 60 (seconds)

*Setter method:* setAutoRebalanceInterval(to: Boolean)

*Description*: The "autoRebalanceInterval" parameter determines how frequently Ordasity should schedule rebalancing of this node's workload, if auto-rebalancing is enabled. If auto-rebalancing is not enabled, this parameter has no effect.

#### drainTime
*Default:* 60 (seconds)

*Setter method:* setDrainTime(to: Int)

*Description*: The "drainTime" parameter determines the period over which Ordasity should release work units to the rest of the cluster during a rebalance or upon shutdown. For example, if 15 work units were to be released over a 60-second period (drainTime == 60), the library would release one work unit every four seconds.


#### useSmartBalancing
*Default*: false

*Setter method:* useSmartBalancing(to: Boolean)

*Description*: The "useSmartBalancing" parameter determines whether Ordasity should use "load-based" (smart) balancing, or "count-based" (simple) balancing. Load-based balancing attempts to distribute load by the amount of "work" required for each work unit, ideally resulting in an even CPU/IO load throughout the cluster. See *Section 5: Rebalancing* in the primary Readme for more information on smart balancing.

**Note:** If you enable smart balancing, be sure to initialize your Cluster with a SmartListener rather than a ClusterListener.

#### zkTimeout
*Default*: 3000 (ms)

*Setter method:* setZKTimeout(to: Int)

*Description*: The "zkTimeout" parameter determines the timeout to be passed to the Zookeeper client library. If the connection to Zookeeper times out, the node will consider itself disconnected from the cluster. Ordasity passes this parameter directly to the Zookeeper client; it is not used otherwise.

#### workUnitName and workUnitShortName
*Default*: "work-units" and "work," respectively

*Setter methods*: setWorkUnitName(to: String) and setWorkUnitShortName(to: String)

*Description*: The workUnitName and workUnitShortName parameters allow you to specify a specific name to be given to the type of work being performed by the cluster. These parameters determine the paths to be used in Zookeeper for fetching a list of work units. By default, Ordasity will look for work units at "/work-units". If workUnitName is set to something else (such as "shards"), Ordasity will look for them at "/shards". Both of these methods can be considered "sugar" in that they're primarily used to tie configuration in Zookeeper to your specific application more closely, and for friendlier logging.

#### nodeId
*Default*: InetAddress.getLocalHost().getHostName() (i.e., the system's hostname)

*Setter method*: setNodeId(to: String)

*Description*: The "nodeId" parameter determines how a node in an Ordasity cluster should identify itself to others in the cluster. This defaults to the system's hostname, but you are welcome to set a custom name if you wish.

#### useSoftHandoff
*Default*: false

*Setter method*: setUseSoftHandoff(to: Boolean)

*Description*: The "useSoftHandoff" parameter determines whether or not a node in the cluster should attempt to hand off work to other nodes in the cluster before shutting down a work unit when it is being drained for shutdown or during a rebalance. If enabled, when releasing a work unit to another node in the cluster, Ordasity will initiate a handoff and ensure a period of overlap (see handoffShutdownDelay below) before calling stopWork on your listener.

#### handoffShutdownDelay
*Default*: 10 (seconds)

*Setter method:* setHandoffShutdownDelay

*Description*: The "setHandoffShutdownDelay" parameter determines the overlap period for a handoff operation. More specifically, when one node hands off a work unit to another, this parameter controls the amount of time the original node should continue serving a work unit before calling "stopWork" on your listener.

---

## ClusterListener and SmartListener
#### Class Name: com.boundary.ordasity.{ClusterListener, SmartListener}

When you initialize a new Cluster, you must supply a "listener" which Ordasity uses to direct your application's workload. ClusterListener and SmartListener are nearly identical, with the sole difference being that ClusterListener is for use with Ordasity's "simple" or count-based load balancing strategy, and SmartListener is for use with Ordasity's "smart" or load-based balancing strategy. In terms of implementation, the only difference is that SmartListener will hand you a Meter to mark as your application performs work.

In general, we recommend enabling Smart Balancing and initializing your cluster with SmartListeners.

Here's how to implement a **ClusterListener**:

    val listener = new ClusterListener {
      // Called when the node has joined the cluster
      def onJoin(client: ZooKeeperClient) = { }

	  // Called when this node should begin serving a work unit
      def startWork(workUnit: String) { }

	  // Called when this node should stop serving a work unit 
      def shutdownWork(workUnit: String) = { }

	  // Called when this node has left the cluster.
      def onLeave() = { }
    }

Here's how to implement a **SmartListener**:

    val listener = new ClusterListener {
      // Called when the node has joined the cluster
      def onJoin(client: ZooKeeperClient) = { }

	  // Called when this node should begin serving a work unit
      def startWork(workUnit: String, meter: Meter) { }

	  // Called when this node should stop serving a work unit 
      def shutdownWork(workUnit: String) = { }

	  // Called when this node has left the cluster.
      def onLeave() = { }
    }

As your application performs work (be it processing an event, serving a query, or handling a request), just call meter.mark() (or meter.mark(someAmount: Int)) to indicate to Ordasity how much "work" is actually being done in service of each work item.

---

## JMX

Ordasity exposes several metrics via JMX for runtime instrumentation. These metrics include the share of the cluster's load this node is serving (if smart balancing is enabled), the number of work units being served by this node, and a list of these work units. They'll be located in JConsole under "com.boundary.ordasity.Cluster".

Finally, Ordasity exposes the "join()" and "shutdown()" methods of Cluster via JMX to allow for remote management of your application. These methods are located at *serviceName.Cluster* in JConsole.

These methods are useful for removing a node from your cluster without exiting the process for maintenance, and for "draining" a node before restarting it during a deploy.

**Here is an example that triggers a drain and shutdown:**

    import java.util.Hashtable
    import javax.management.ObjectName
    import javax.management.remote.JMXServiceURL
    import javax.management.remote.JMXConnectorFactory

    val jmxPort = "8083"
    val jmxHost = "localhost"
    val serviceName = "example"
    val workUnitShortName = "work"

    val jmxUrl = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + jmxHost + ":" + jmxPort + "/jmxrmi")
    val remote = JMXConnectorFactory.connect(jmxUrl).getMBeanServerConnection

    println("Invoking shutdown...")
    remote.invoke(new ObjectName(serviceName + ":name=Cluster"), "shutdown", Array[Object](), Array[String]())

    val hash = new Hashtable[String, String]
    hash.put("type", "Cluster")
    hash.put("name", "my_" + workUnitShortName + "_count")

    var workUnitCount = remote.getAttribute(new ObjectName("com.boundary.ordasity", hash), "Value").asInstanceOf[Int]  
    while (workUnitCount > 0) {
      workUnitCount = remote.getAttribute(new ObjectName("com.boundary.ordasity", hash), "Value").asInstanceOf[Int]
      println("Waiting for drain to complete. Remaining work units: " + workUnitCount)
      Thread.sleep(2000)
    }

    println("Graceful handoff complete. Node shut down.")