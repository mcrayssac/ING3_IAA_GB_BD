package nyctaxi.publication

import java.io.IOException
import java.nio.file.AccessDeniedException
import java.time.Duration
import java.util.concurrent.{Callable, CountDownLatch, ExecutionException, Executors, TimeUnit,
  TimeoutException}
import nyctaxi.shared.Retry
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.awscore.exception.AwsServiceException
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/** Test-injectable connection timeout, per-attempt deadline, and retry budget of storage operations. */
final case class StoragePolicy(
  connectTimeout: Duration = Duration.ofSeconds(30), operationTimeout: Duration = Duration.ofMinutes(5),
  attempts: Int = 3, backoffMillis: Long = 1000L
)

final class PublicationConflict(message: String) extends IllegalStateException(message)
final class StorageAccessFailure extends IOException("Storage authentication or access denied")
private final class UnstoppedOperation
  extends IllegalStateException("Timed-out storage operation did not stop safely")

/** Cancellation closes active resources before a retry can start. */
final class OperationScope {
  private var cancelled = false
  private val cleanup = ArrayBuffer.empty[() => Unit]
  def check(): Unit = synchronized {
    if (cancelled || Thread.currentThread().isInterrupted)
      throw new InterruptedException("Storage operation interrupted")
  }
  def onCancel(action: () => Unit): Unit = synchronized {
    if (cancelled) { action(); throw new InterruptedException("Storage operation interrupted") }
    cleanup += action
  }
  def cancel(): Unit = {
    val actions = synchronized { cancelled = true; cleanup.reverse.toVector }
    actions.foreach(action => try action() catch { case NonFatal(_) => () })
  }
}

object StorageFailures {
  private def causes(error: Throwable): Vector[Throwable] = {
    val result = ArrayBuffer.empty[Throwable]
    var current = error
    while (current != null && !result.exists(_ eq current)) { result += current; current = current.getCause }
    result.toVector
  }

  /** Authentication and authorization failures, which no retry can fix. */
  def denied(error: Throwable): Boolean = causes(error).exists {
    case _: AccessDeniedException | _: StorageAccessFailure => true
    case service: AwsServiceException => Set(401, 403).contains(service.statusCode())
    case _ => false
  }

  /** Throttling, server errors, and transport failures. Conflicts and invalid input are permanent. */
  def retryable(error: Throwable): Boolean = {
    val chain = causes(error)
    val permanent = chain.exists {
      case _: IllegalArgumentException | _: PublicationConflict | _: UnstoppedOperation => true
      case _ => false
    }
    if (denied(error) || permanent) false
    else chain.collectFirst { case service: AwsServiceException =>
      service.statusCode() == 429 || service.statusCode() >= 500
    }.getOrElse(error.isInstanceOf[IOException] || error.isInstanceOf[TimeoutException] ||
      error.isInstanceOf[SdkClientException])
  }
}

/** Deadlines, cancellation, and retries of storage operations (interfaces I3 and I11, task M2.3).
  * Deadlines cover the entire operation, including streamed response bodies and stream close.
  *
  * Input: one storage operation that registers its open resources with an `OperationScope`.
  * Output: the operation's result, with at most `attempts` tries for transient failures.
  * Failure: access denial becomes `StorageAccessFailure` immediately. Conflicts and invalid input are never
  *   retried. A timed-out operation that does not stop throws `UnstoppedOperation`.
  */
final class StorageOperations(policy: StoragePolicy = StoragePolicy()) {
  /** Runs one operation with a deadline per attempt and bounded retries of transient failures. */
  def run[A](operation: OperationScope => A): A =
    try Retry.run(policy.attempts, policy.backoffMillis, "Publication")(StorageFailures.retryable)(
      once(operation))
    catch { case error if StorageFailures.denied(error) => throw new StorageAccessFailure }

  /** One attempt on a dedicated thread, cancelled and cleaned up when its deadline expires. */
  private def once[A](operation: OperationScope => A): A = {
    val scope = new OperationScope()
    val finished = new CountDownLatch(1)
    // LIMIT: one thread per attempt. Fine for a few large objects, costly for thousands of small ones.
    val executor = Executors.newSingleThreadExecutor((task: Runnable) => {
      val thread = new Thread(task, "tlc-storage-operation")
      thread.setDaemon(true)
      thread
    })
    val future = executor.submit(new Callable[A] {
      override def call(): A = try operation(scope) finally finished.countDown()
    })
    try future.get(policy.operationTimeout.toMillis, TimeUnit.MILLISECONDS) catch {
      case error: ExecutionException => throw error.getCause
      case timeout: TimeoutException =>
        future.cancel(true)
        scope.cancel()
        // LIMIT: a cancelled operation gets 5 s to stop. A slower one fails the run without retry.
        if (!finished.await(5, TimeUnit.SECONDS)) throw new UnstoppedOperation
        throw timeout
      case interrupted: InterruptedException =>
        future.cancel(true)
        scope.cancel()
        throw interrupted
    } finally executor.shutdownNow()
  }
}
