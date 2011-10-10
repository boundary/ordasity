import java.util.Random
import com.yammer.metrics.Meter
import java.util.concurrent.CountDownLatch
import com.boundary.cluster.{Cluster, ClusterConfig, ClusterListener, SmartListener}
import com.codahale.logula.Logging
import com.yammer.metrics.Meter
import com.twitter.zookeeper.ZooKeeperClient

Logging.configure

val random = new Random()
val latch = new CountDownLatch(1)

val config = new ClusterConfig("localhost:2181").
  setAutoRebalance(true).
  setRebalanceInterval(15).
  useSmartBalancing(false).
  setDrainTime(3).
  setZKTimeout(3).
  setUseSoftHandoff(true).
  setNodeId(java.util.UUID.randomUUID().toString)

val listener = new SmartListener {
  def onJoin(client: ZooKeeperClient) = {}
  def onLeave() = {}

  // Do yer thang, mark dat meter.
  def startWork(workUnit: String, meter: Meter) = {
	//meter.mark(random.nextInt(1000))
  }

  // Stop doin' that thang
  def shutdownWork(workUnit: String) = null
}

val clustar = new Cluster("example_service", listener, config)

clustar.join()
latch.await()
