package com.boundary.ordasity

import java.util.concurrent.BlockingQueue

import org.slf4j.LoggerFactory

/**
 * Thread responsible for claiming work in Ordasity. This thread waits for a
 * claim token to arrive (see TokenQueue for explanation), then performs an
 * Ordasity claim cycle for unclaimed work units.
 */
class Claimer(cluster: Cluster, name: String = "ordasity-claimer") extends Thread(name) {

  val log = LoggerFactory.getLogger(getClass)
  private val claimQueue : BlockingQueue[ClaimToken] = new TokenQueue
  def requestClaim() : Boolean = claimQueue.offer(ClaimToken.token)

  override def run() {
    log.info("Claimer started.")
    try {
      while (cluster.getState() != NodeState.Shutdown) {
        claimQueue.take()
        try {
          cluster.claimWork()
        } catch {
          case e: InterruptedException =>
            // Don't swallow these
            throw e
          case e: Exception =>
            log.error("Claimer failed to claim work", e)
        }
      }
    } catch {
      case e: Throwable =>
        log.error("Claimer failed unexpectedly", e)
        throw e
    } finally {
      log.info("Claimer shutting down.")
    }
  }

}
