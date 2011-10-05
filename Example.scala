import java.util.Random
import com.yammer.metrics.Meter
import java.util.concurrent.CountDownLatch
import cluster.{Cluster, ClusterConfig, ClusterListener, SmartListener}
import com.codahale.logula.Logging

Logging.configure

val random = new Random()
val latch = new CountDownLatch(1)

val config = new ClusterConfig("localhost:2181").
  setAutoRebalance(true).
  setRebalanceInterval(10).
  useSmartBalancing(false).
  setDrainTime(3).
  setZKTimeout(3).
  setMultitenant(true)

val listener = new ClusterListener {
  // Do yer thang, mark dat meter.
  def startWork(workUnit: String) = {
	//meter.mark(random.nextInt(1000))
  }

  // Stop doin' that thang
  def shutdownWork(workUnit: String) = null
}

val clustar = new Cluster("ServiceName", listener, config)

clustar.join()
latch.await()
