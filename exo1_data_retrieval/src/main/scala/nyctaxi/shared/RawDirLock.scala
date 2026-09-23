package nyctaxi.shared

import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption}
import scala.util.Using

/** Exclusive use of RAW_DIR by one retrieval or publication process (interface I2).
  *
  * Input: an existing RAW_DIR and the message to report when it is busy.
  * Output: the result of the guarded body.
  * Failure: `IllegalArgumentException` with `busyMessage` when another process holds the lock.
  *   The OS releases the lock after a crash.
  */
object RawDirLock {
  /** Runs `body` while holding the `.retrieval.lock` file lock of RAW_DIR. */
  def hold[A](rawDir: Path, busyMessage: String)(body: => A): A = {
    val file = rawDir.resolve(".retrieval.lock")
    // LIMIT: advisory OS lock on one host. Processes on another machine sharing RAW_DIR are not excluded.
    Using.resource(FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) { channel =>
      val lock = channel.tryLock()
      require(lock != null, busyMessage)
      try body finally lock.release()
    }
  }
}
