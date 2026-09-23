package nyctaxi.shared

/** Bounded retries of transient failures, shared by HTTP retrieval and RustFS publication.
  *
  * Input: an attempt count, a linear backoff with an optional cap, a retryable predicate, and one attempt.
  * Output: the result of the first successful attempt.
  * Failure: a non-retryable error, or the error of the last attempt, is rethrown unchanged.
  *   An interrupted thread stops before the next attempt with `"<context> interrupted"`.
  */
object Retry {
  /** Runs `attempt` until it succeeds, fails permanently, or has used all `attempts`. */
  def run[A](attempts: Int, backoffMillis: Long, context: String, maxBackoffMillis: Long = Long.MaxValue)
    (retryable: Throwable => Boolean)(attempt: => A): A = {
    var number = 1
    while (true) {
      Interrupts.check(context)
      try return attempt catch {
        case error if retryable(error) && number < attempts =>
          // LIMIT: linear backoff without jitter. Concurrent clients can retry in lockstep.
          Thread.sleep(math.min(maxBackoffMillis, backoffMillis * number))
          number += 1
      }
    }
    throw new IllegalStateException("Unreachable retry state")
  }
}
