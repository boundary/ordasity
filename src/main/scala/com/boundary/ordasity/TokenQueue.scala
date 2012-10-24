package com.boundary.ordasity

import java.util.concurrent.LinkedBlockingQueue
import com.yammer.metrics.scala.Instrumented

class ClaimToken()
object ClaimToken { val token = new ClaimToken }

/**
 * Implementation of a BlockingQueue that operates via a "claim token."
 * The desired properties for this queue are one that works like a set, collapsing
 * duplicate events into a single one. While contains() is an O(N) operation, N will
 * equal at most one. An alternative would be an Object.wait() / Object.notify() impl,
 * but it seems desirable to stick with j/u/c classes. This implementation will
 * also allow for other types of events aside from a ClaimToken, if necessary.
 */
class TokenQueue[ClaimToken] extends LinkedBlockingQueue[ClaimToken] with Instrumented {
  val suppressedMeter = metrics.meter("ordasity", "suppressedClaimCycles")
  val requestedMeter = metrics.meter("ordasity", "claimCycles")

  override def offer(obj: ClaimToken) : Boolean = {
    if (contains(obj)) {
      suppressedMeter.mark()
      false
    } else {
      requestedMeter.mark()
      super.offer(obj)
    }
  }
}
