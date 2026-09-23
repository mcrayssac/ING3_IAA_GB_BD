package nyctaxi.shared

import java.io.IOException
import org.scalatest.funsuite.AnyFunSuite

/** Retries stop at the attempt limit, on permanent failures, and on interruption. */
class RetrySpec extends AnyFunSuite {
    private def transient(error: Throwable): Boolean = error.isInstanceOf[IOException]

    test("transient failures are retried up to the limit, then the last error is rethrown") {
        var calls = 0
        val error = intercept[IOException](Retry.run(3, 1L, "Test")(transient) {
            calls += 1; throw new IOException(s"$calls")
        })
        assert(calls == 3 && error.getMessage == "3")
        calls = 0
        assert(Retry.run(3, 1L, "Test")(transient) {
            calls += 1; if (calls < 2) throw new IOException("once") else "ok"
        } == "ok")
        assert(calls == 2)
    }

    test("permanent failures are not retried and the backoff cap bounds each wait") {
        var calls = 0
        intercept[IllegalStateException](Retry.run(3, 1L, "Test")(transient) {
            calls += 1; throw new IllegalStateException()
        })
        assert(calls == 1)
        val started = System.nanoTime()
        intercept[IOException](Retry.run(
            2,
            60000L,
            "Test",
            maxBackoffMillis = 5L
        )(transient)(throw new IOException()))
        assert(System.nanoTime() - started < 5000000000L)
    }

    test("an interrupted thread stops before the next attempt with the context in the message") {
        Thread.currentThread().interrupt()
        try assert(intercept[InterruptedException](Retry.run(
                3,
                1L,
                "Upload"
            )(transient)("never")).getMessage ==
                "Upload interrupted")
        finally Thread.interrupted()
    }
}
