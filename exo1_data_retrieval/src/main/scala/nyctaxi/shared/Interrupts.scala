package nyctaxi.shared

/** Cooperative cancellation points shared by retrieval and publication. */
object Interrupts {
    /** Stops the current step once the thread is interrupted, naming the interrupted activity. */
    def check(context: String): Unit =
        if (Thread.currentThread().isInterrupted) throw new InterruptedException(s"$context interrupted")
}
