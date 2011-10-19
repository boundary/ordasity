import java.util.Random
import java.util.concurrent.CountDownLatch
import com.boundary.ordasity.{Cluster, ClusterConfig, SmartListener}
import com.codahale.logula.Logging
import com.yammer.metrics.Meter
import com.twitter.zookeeper.ZooKeeperClient
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit, ScheduledFuture}
import java.util.{HashMap, TimerTask}

Logging.configure

val random = new Random()
val latch = new CountDownLatch(1)
val pool = new ScheduledThreadPoolExecutor(1)

val futures = new HashMap[String, ScheduledFuture[_]]

val config = new ClusterConfig("localhost:2181").
  setAutoRebalance(true).
  setRebalanceInterval(15).
  useSmartBalancing(true).
  setDrainTime(3).
  setZKTimeout(3).
  setUseSoftHandoff(true).
  setNodeId(java.util.UUID.randomUUID().toString)

val listener = new SmartListener {
  def onJoin(client: ZooKeeperClient) = {}
  def onLeave() = {}

  // Do yer thang, mark dat meter.
  def startWork(workUnit: String, meter: Meter) = {
    val task = new TimerTask {
      def run() = meter.mark(random.nextInt(1000))
    }
	  val future = pool.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS)
	  futures.put(workUnit, future)
  }

  // Stop doin' that thang
  def shutdownWork(workUnit: String) {
    futures.get(workUnit).cancel(true)
  }
}

val clustar = new Cluster("example_service", listener, config)

clustar.join()
latch.await()
