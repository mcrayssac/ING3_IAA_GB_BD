package nyctaxi.shared

import java.io.{InputStream, OutputStream}
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import scala.util.Using

/** SHA-256 of files, memory, and streams in bounded memory.
  *
  * Input: bytes from disk, memory, or a stream, optionally copied to an output while hashing.
  * Output: the lowercase hexadecimal SHA-256 and, for streams, the byte count.
  * Failure: I/O errors propagate. The `check` callback can abort between chunks, for example on interruption.
  */
object Digest {
    private val chunkSize = 64 * 1024

    /** Hashes small in-memory content such as sidecars, descriptors, and receipts. */
    def sha256(bytes: Array[Byte]): String = hex(MessageDigest.getInstance("SHA-256").digest(bytes))

    /** Hashes disk bytes, including files reused without network access. */
    def sha256(path: Path): String =
        Using.resource(Files.newInputStream(path))(input =>
            stream(input, () => Interrupts.check("Hashing"))._2
        )

    /** Hashes a stream while optionally copying it, calling `check` before each chunk and at the end. */
    def stream(input: InputStream, check: () => Unit, output: Option[OutputStream] = None): (Long, String) = {
        val digest = MessageDigest.getInstance("SHA-256")
        val buffer = new Array[Byte](chunkSize)
        var total = 0L
        var count = input.read(buffer)
        while (count != -1) {
            check()
            digest.update(buffer, 0, count)
            output.foreach(_.write(buffer, 0, count))
            total += count
            count = input.read(buffer)
        }
        check()
        (total, hex(digest.digest()))
    }

    private def hex(bytes: Array[Byte]): String = bytes.map(b => f"${b & 0xff}%02x").mkString
}
