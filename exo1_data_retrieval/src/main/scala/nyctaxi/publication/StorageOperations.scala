package nyctaxi.publication

import java.io.{IOException, InputStream, OutputStream}
import java.nio.file.AccessDeniedException
import java.security.MessageDigest
import java.time.Duration
import java.util.concurrent.{Callable, CountDownLatch, ExecutionException, Executors, TimeUnit, TimeoutException}
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.awscore.exception.AwsServiceException
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

final case class StoragePolicy(
  connectTimeout: Duration = Duration.ofSeconds(30), operationTimeout: Duration = Duration.ofMinutes(5),
  attempts: Int = 3, backoffMillis: Long = 1000L
)

final class PublicationConflict(message: String) extends IllegalStateException(message)
final class StorageAccessFailure extends IOException("Storage authentication or access denied")
private final class UnstoppedOperation extends IllegalStateException("Timed-out storage operation did not stop safely")

/** Cancellation closes active resources before a retry can start. */
final class OperationScope {
  private var cancelled = false
  private val cleanup = ArrayBuffer.empty[() => Unit]
  def check(): Unit = synchronized {
    if (cancelled || Thread.currentThread().isInterrupted) throw new InterruptedException("Storage operation interrupted")
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
  def denied(error: Throwable): Boolean = causes(error).exists {
    case _: AccessDeniedException | _: StorageAccessFailure => true
    case service: AwsServiceException => Set(401, 403).contains(service.statusCode())
    case _ => false
  }
  def retryable(error: Throwable): Boolean = {
    val chain = causes(error)
    if (denied(error) || chain.exists(_.isInstanceOf[IllegalArgumentException]) ||
      chain.exists(_.isInstanceOf[PublicationConflict]) || chain.exists(_.isInstanceOf[UnstoppedOperation])) false
    else chain.collectFirst { case service: AwsServiceException =>
      service.statusCode() == 429 || service.statusCode() >= 500
    }.getOrElse(error.isInstanceOf[IOException] || error.isInstanceOf[TimeoutException] || error.isInstanceOf[SdkClientException])
  }
}

/** Deadlines cover the entire operation, including streamed response bodies and stream close. */
final class StorageOperations(policy: StoragePolicy = StoragePolicy()) {
  def run[A](operation: OperationScope => A): A = {
    var attempt = 1
    while (true) {
      if (Thread.currentThread().isInterrupted) throw new InterruptedException("Publication interrupted")
      try return once(operation) catch {
        case error if StorageFailures.denied(error) => throw new StorageAccessFailure
        case error if StorageFailures.retryable(error) && attempt < policy.attempts =>
          Thread.sleep(policy.backoffMillis * attempt)
          attempt += 1
      }
    }
    throw new IllegalStateException("Unreachable retry state")
  }

  private def once[A](operation: OperationScope => A): A = {
    val scope = new OperationScope()
    val finished = new CountDownLatch(1)
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
        if (!finished.await(5, TimeUnit.SECONDS)) throw new UnstoppedOperation
        throw timeout
      case interrupted: InterruptedException =>
        future.cancel(true)
        scope.cancel()
        throw interrupted
    } finally executor.shutdownNow()
  }
}

object StreamDigest {
  /** One bounded buffer computes the digest while copying or independently reading remote bytes. */
  def read(input: InputStream, scope: OperationScope, output: Option[OutputStream] = None): (Long, String) = {
    val digest = MessageDigest.getInstance("SHA-256")
    val buffer = new Array[Byte](64 * 1024)
    var total = 0L
    var count = input.read(buffer)
    while (count != -1) {
      scope.check()
      digest.update(buffer, 0, count)
      output.foreach(_.write(buffer, 0, count))
      total += count
      count = input.read(buffer)
    }
    scope.check()
    (total, digest.digest().map(b => f"${b & 0xff}%02x").mkString)
  }
}
