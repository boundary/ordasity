# Ordasity

## Table of Contents
1. Overview, Use Cases, and Features
2. A Clustered Service in 30 Seconds
3. In Action at Boundary
4. Distribution / Coordination Strategy
5. Rebalancing
6. Draining and Handoff
7. Wrapping Up
8. [API Documentation](https://github.com/boundary/ordasity/wiki/Ordasity-API-Documentation)


## Building Stateful Clustered Services on the JVM

Ordasity is a library designed to make building and deploying reliable clustered services on the JVM as straightforward as possible. It's written in Scala and uses Zookeeper for coordination.

Ordasity's simplicity and flexibility allows us to quickly write, deploy, and (most importantly) operate distributed systems on the JVM without duplicating distributed "glue" code or revisiting complex reasoning about distribution strategies.

---

### Primary Use Cases

Ordasity is designed to spread persistent or long-lived workloads across several machines. It's a toolkit for building systems which can be described in terms of individual nodes serving a partition or shard of a cluster's total load. Ordasity is not designed to express a "token range" (though it may be possible to implement one); the focus is on discrete work units.

---

### Features
- Cluster membership (joining / leaving / mutual awareness)
- Work claiming and distribution
- Load-based workload balancing
- Count-based workload balancing
- Automatic periodic rebalancing
- Graceful cluster exiting ("draining")
- Graceful handoff of work units between nodes
- Pegging of work units to a specific node

---

### A Clustered Service in 30 Seconds

Let's get started with an example. Here's how to build a clustered service in 25 lines of code with Ordasity:

```scala
    import com.yammer.metrics.scala.Meter
    import com.twitter.common.zookeeper.ZooKeeperClient
    import com.boundary.ordasity.{Cluster, ClusterConfig, SmartListener}

    class MyService {
      val listener = new SmartListener {
		
		// Called after successfully joining the cluster.
        def onJoin(client: ZooKeeperClient) { } 

        // Do yer thang, mark that meter.
        def startWork(workUnit: String, meter: Meter) { }

        // Stop doin' that thang.
        def shutdownWork(workUnit: String) { }

		// Called after leaving the cluster.
        def onLeave() { }
      }

      val config = ClusterConfig.builder().setHosts("localhost:2181").build()
      val cluster = new Cluster("ServiceName", listener, config)

      cluster.join()
    }
```

**Maven** folks and friends with compatible packaging systems, here's the info for your pom.xml:

```xml
        <!-- Dependency -->
        <dependency>
            <groupId>com.boundary</groupId>
            <artifactId>ordasity-scala_2.9.1</artifactId>
            <version>0.4.5</version>
        </dependency>

        <!-- Repo -->
        <repository>
            <id>boundary-public</id>
            <name>Boundary Public</name>
            <url>http://maven.boundary.com/artifactory/external</url>
        </repository>
```

---

### In Action at Boundary

At Boundary, the library holds together our pubsub and event stream processing systems. It's a critical part of ensuring that at any moment, we're consuming and aggregating data from our network of collectors at one tier, and processing this data at hundreds of megabits a second in another. Ordasity also helps keep track of the mappings between these services, wiring everything together for us behind the scenes.

Ordasity's distribution enables us to spread the work of our pubsub aggregation and event stream processing systems across any number of nodes. Automatic load balancing keeps the cluster's workload evenly distributed, with nodes handing off work to others as workload changes. Graceful draining and handoff allows us to iterate rapidly on these systems, continously deploying updates without disrupting operation of the cluster. Ordasity's membership and work claiming approach ensures transparent failover within a couple seconds if a node becomes unavailable due to a network partition or system failure.

---

### Distribution / Coordination Strategy

Ordasity's architecture is masterless, relying on Zookeeper only for coordination between individual nodes. The service is designed around the principle that many nodes acting together under a common set of rules can cooperatively form a self-organizing, self-regulating system.

Ordasity supports two work claiming strategies: "simple" (count-based), and "smart" (load-based).

#### Count-Based Distribution
The count-based distribution strategy is simple. When in effect, each node in the cluster will attempt to claim its fair share of available work units according to the following formula:

```scala
      val maxToClaim = {
        if (allWorkUnits.size <= 1) allWorkUnits.size
        else (allWorkUnits.size / nodeCount.toDouble).ceil
      }
```

If zero or one work units are present, the node will attempt to claim up to one work unit. Otherwise, the node will attempt to claim up to the number of work units divided by the number of active nodes.

#### Load-Based Distribution
Ordasity's load-based distribution strategy assumes that all work units are not equal. It's unlikely that balancing simply by count will result in an even load distribution -- some nodes would probably end up much busier than others. The load-based strategy is smarter. It divides up work based on the amount of actual "work" done.


##### Meters Measure Load
When you enable smart balancing and initialize Ordasity with a SmartListener, you get back a "meter" to mark when work occurs. Here's a simple, contrived example:

```scala
    val listener = new SmartListener {
      ...
      def startWork(workUnit: String, meter: Meter) = {

        val somethingOrOther = new Runnable() {
          def run() {
            while (true) {
              val processingAmount = process(workUnit)
              meter.mark(processingAmount)
              Thread.sleep(100)
            }
          }
        }

        new Thread(somethingOrOther).start()
      }
  
      ...
    }
```

Ordasity uses this meter to determine how much "work" each work unit in the cluster represents. If the application were a database or frontend to a data service, you might mark the meter each time a query is performed. In a messaging system, you'd mark it each time a message is sent or received. In an event stream processing system, you'd mark it each time an event is processed. You get the idea.

*(Bonus: Each of these meters expose their metrics via JMX, providing you and your operations team with insight into what's happening when your service is in production).*

##### Knowing the Load Lets us Balance
Ordasity checks the meters once per minute (configurable) and updates this information in Zookeeper. The "load map" determines the actual load represented by each work unit. All nodes watch the cluster's "load map" and are notified via Zookeeper's Atomic Broadcast mechanism when this changes. Each node in the cluster will attempt to claim its fair share of available work units according to the following formula:

```scala
    def evenDistribution() : Double = {
      loadMap.values.sum / activeNodeSize().toDouble
    }
```

As the number of nodes or the load of individual work units change, each node's idea of an "even distribution" changes as well. Using this "even distribution" value, each node will choose to claim additional work, or in the event of a rebalance, drain its workload to other nodes if it's processing more than its fair share.

---

### Rebalancing

Ordasity supports automatic and manual rebalancing to even out the cluster's load distribution as workloads change.

To trigger a manual rebalance on all nodes, touch "/service-name/meta/rebalance" in Zookeeper. However, automatic rebalancing is preferred. To enable it, just turn it on in your cluster config:

```scala
    val config = ClusterConfig.builder().
      setHosts("localhost:2181").
      setAutoRebalance(true).
      setRebalanceInterval(60 * 60).build() // One hour
```

As a masterless service, the rebalance process is handled uncoordinated by the node itself. The rebalancing logic is very simple. If a node has more than its fair share of work when a rebalance is triggered, it will drain or release this work to other nodes in the cluster. As the cluster sees this work become available, lighter-loaded nodes will claim it (or receive handoff) and begin processing.

If you're using **count-based distribution**, it looks like this:

```scala
    def simpleRebalance() {
      val target = fairShare()

      if (myWorkUnits.size > target) {
        log.info("Simple Rebalance triggered. Load: %s. Target: %s.",  myWorkUnits.size, target)
        drainToCount(target)
      }
    }
```

If you're using **load-based distribution**, it looks like this:

```scala
    def smartRebalance() {
      val target = evenDistribution()

      if (myLoad() > target) {
        log.info("Smart Rebalance triggered. Load: %s. Target: %s", myLoad(), target)
        drainToLoad(target.longValue)
      }
    }
```

---

### Draining and Handoff

To avoid dumping a bucket of work on an already-loaded cluster at once, Ordasity supports "draining." Draining is a process by which a node can gradually release work to other nodes in the cluster. In addition to draining, Ordasity also supports graceful handoff, allowing for a period of overlap during which a new node can begin serving a work unit before the previous owner shuts it down.

#### Draining

Ordasity's work claiming strategies (count-based and load-based) have internal counterparts for releasing work: *drainToLoad* and *drainToCount*.

The *drainToCount* and *drainToLoad* strategies invoked by a rebalance will release work units until the node's load is just greater than its fair share. That is to say, each node is "generous" in that it will strive to maintain slightly greater than a mathematically even distribution of work to guard against a scenario where work units are caught in a cycle of being claimed, released, and reclaimed continually. (Similarly, both claiming strategies will attempt to claim one unit beyond their fair share to avoid a scenario in which a work unit is claimed by no one).

Ordasity allows you to configure the period of time for a drain to complete: 

```scala
    val config = ClusterConfig.builder().setHosts("localhost:2181").setDrainTime(60).build() // 60 Seconds
```

When a drain is initiated, Ordasity will pace the release of work units over the time specified. If 15 work units were to be released over a 60-second period, the library would release one every four seconds.

Whether you're using count-based or load-based distribution, the drain process is the same. Ordasity makes a list of work units to unclaim, then paces their release over the configured drain time.

Draining is especially useful for scheduled maintenance and deploys. Ordasity exposes a "shutdown" method via JMX. When invoked, the node will set its status to "Draining," cease claiming new work, and release all existing work to other nodes in the cluster over the configured interval before exiting the cluster.

#### Handoff
When Handoff is enabled, Ordasity will allow another node to begin processing for a work unit before the former owner shuts it down. This eliminates the very brief gap between one node releasing and another node claiming a work unit. Handoff ensures that at any point, a work unit is being served.

To enable it, just turn it on in your ClusterConfig:

```scala
    val clusterConfig = ClusterConfig.builder().
      setHosts("localhost:2181").
      setUseSoftHandoff(true).
      setHandoffShutdownDelay(10).build() // Seconds
```

The handoff process is fairly straightforward. When a node has decided to release a work unit (either due to a rebalance or because it is being drained for shutdown), it creates an entry in Zookeeper at /service-name/handoff-requests. Following their count-based or load-based claiming policies, other nodes will claim the work being handed off by creating an entry at /service-name/handoff-results.

When a node has successfully accepted handoff by creating this entry, the new owner will begin work. The successful "handoff-results" entry signals to the original owner that handoff has occurred and that it is free to cease processing after a configurable overlap (default: 10 seconds). After this time, Ordasity will call the "shutdownWork" method on your listener.

---

### Registering work units

Work units are registered by creating ZooKeeper nodes under `/work-units`. (If you have set `Cluster.workUnitName` to a custom value then this ZooKeeper path will change accordingly.)

The name of the work unit is the same as the name of the ZooKeeper node. So, for example to create 3 work units called "a", "b", and "c", your ZK directory should look like this:

    /work-units
        /a
        /b
        /c

Any String that is a valid ZK node name can be used as a work unit name. This is the string that is passed to your `ClusterListener` methods.

The ZK node data must be a JSON-encoded `Map[String, String]`. This may be simply an empty map (`{}`), or you may want to include information about the work unit, for use by your cluster nodes.

Note that Ordasity does not pass the ZK node data to your `ClusterListener`, so you will have to retrieve it yourself using the ZK client. It also does not provide a helper to deserialize the JSON string.

#### Pegging

The ZK node data can also be used for pegging work units to specific nodes. 

To do this, include a key-value pair of the form `"servicename": "nodeId"` in the JSON map. 

Here `servicename` is the name of the cluster, as specified in `Cluster`'s constructor, and `nodeId` is the unique ID of a node, as set in `ClusterConfig`.

For example to peg a work unit to Node `node123` in cluster `mycluster`, set the ZK node's data to `{"mycluster": "node123"}`.

---

### Wrapping Up

So, that's Ordasity! We hope you enjoy using it to build reliable distributed services quickly.

#### Questions
If you have any questions, please feel free to shoot us an e-mail or get in touch on Twitter.

#### Bug Reports and Contributions
Think you've found a bug? Sorry about that. Please open an issue on GitHub and we'll check it out as soon as possible.

Want to contribute to Ordasity? Awesome! Fork the repo, make your changes, and issue a pull request. Please make effort to keep commits small, clean, and confined to specific changes. If you'd like to propose a new feature, give us a heads-up by getting in touch beforehand. We'd like to talk with you.






