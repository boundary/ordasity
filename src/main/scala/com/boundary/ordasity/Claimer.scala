package com.boundary.ordasity

import java.util.concurrent.BlockingQueue
import com.codahale.logula.Logging

/**
 * Thread responsible for claiming work in Ordasity. This thread waits for a
 * claim token to arrive (see TokenQueue for explanation), then performs an
 * Ordasity claim cycle for unclaimed work units.
 */
class Claimer(cluster: Cluster) extends Thread with Logging {

  private val claimQueue : BlockingQueue[ClaimToken] = new TokenQueue
  def requestClaim() : Boolean = claimQueue.offer(ClaimToken.token)

  override def run() {
    log.info("Claimer started.")
    while (cluster.getState() != NodeState.Shutdown) {
      claimQueue.take()
      cluster.claimWork()
    }

    log.info("Claimer shutting down.")
  }

}
