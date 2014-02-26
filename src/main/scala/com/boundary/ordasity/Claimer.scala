package com.boundary.ordasity

import java.util.concurrent.BlockingQueue
import com.boundary.logula.Logging

/**
 * Thread responsible for claiming work in Ordasity. This thread waits for a
 * claim token to arrive (see TokenQueue for explanation), then performs an
 * Ordasity claim cycle for unclaimed work units.
 */
class Claimer(cluster: Cluster, name: String = "ordasity-claimer") extends Thread(name) with Logging {

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
            log.error(e, "Claimer failed to claim work")
        }
      }
    } catch {
      case e: Throwable =>
        log.error(e, "Claimer failed unexpectedly")
        throw e
    } finally {
      log.info("Claimer shutting down.")
    }
  }

}
