package nyctaxi.retrieval

import java.net.URI
import java.nio.channels.FileChannel
import java.nio.file.{Files, StandardOpenOption}
import scala.util.Using
import scala.util.control.NonFatal

final case class MonthResult(month: String, success: Boolean, message: String)

/** Per-month orchestration is injectable so HTTP failure tests never depend on TLC. */
final class RetrievalRunner(
  downloader: HttpDownload,
  verifier: ParquetVerifier,
  store: ProvenanceStore = new ProvenanceStore(),
  source: String => URI = RetrievalConfig.source,
  log: String => Unit = println
) {
  /** A failed month does not discard completed months or prevent the following one. */
  def run(config: RetrievalConfig): Vector[MonthResult] = {
    Files.createDirectories(config.rawDir)
    // Serialize writers to this staging directory. The OS releases the lock after a crash.
    Using.resource(FileChannel.open(config.rawDir.resolve(".retrieval.lock"),
      StandardOpenOption.CREATE, StandardOpenOption.WRITE)) { channel =>
      val lock = channel.tryLock()
      require(lock != null, "Another retrieval process is using RAW_DIR")
      try config.months.map { month =>
        if (Thread.currentThread().isInterrupted) throw new InterruptedException("Retrieval interrupted")
        val result = try retrieve(config, month) catch {
          case NonFatal(error) => MonthResult(month, false, Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
        }
        log(s"${result.month}: ${if (result.success) "OK" else "FAILED"} ${result.message}")
        result
      } finally lock.release()
    }
  }

  private def retrieve(config: RetrievalConfig, month: String): MonthResult = {
    val name = RetrievalConfig.filename(month)
    val file = config.rawDir.resolve(name)
    val url = source(month)
    val existing = if (config.refresh) None else store.existing(file, month, url.toString)
    val report = existing match {
      case Some(saved) =>
        val observed = verifier.verify(file)
        require(observed.rowCount == saved.verification.rowCount &&
          observed.parquetSchema == saved.verification.parquetSchema &&
          observed.sparkSchema == saved.verification.sparkSchema, "Verification differs from recorded metadata")
        observed
      case None =>
        val candidate = downloader.fetch(url, config.rawDir, name)
        try {
          val observed = verifier.verify(candidate.path)
          if (Thread.currentThread().isInterrupted) throw new InterruptedException("Verification interrupted")
          val saved = Provenance(month, name, url.toString, candidate.finalUrl, candidate.retrievedAt,
            candidate.bytes, candidate.sha256, observed)
          store.publish(candidate.path, file, saved)
          observed
        } finally Files.deleteIfExists(candidate.path)
    }
    val action = if (existing.nonEmpty) "reused" else if (config.refresh) "refreshed" else "downloaded"
    MonthResult(month, true, s"$action and fully verified $name, ${Files.size(file)} bytes, ${report.rowCount} rows")
  }
}

object LocalRetrieval {
  private val help = """Usage: runMain nyctaxi.retrieval.LocalRetrieval [--refresh | --help]
    |TAXI_MONTHS: comma-separated subset of 2026-05,2026-06,2026-07 (default: all three).
    |RAW_DIR: local staging directory (default: data/raw relative to repository root).
    |SPARK_MASTER: local Spark master (default: local[*]).
    |--refresh downloads and verifies replacements before promoting them.
    |Existing downloads require matching metadata, size, and SHA-256 for normal reuse.
    |Configuration comes from the process environment. .env is not loaded.
    |""".stripMargin

  /** Only this process boundary exits the JVM. Tests call the runner directly. */
  def main(args: Array[String]): Unit = {
    if (args.toVector == Vector("--help")) { println(help); return }
    val status = execute(RetrievalConfig.parse(args, sys.env))
    if (status != 0) System.exit(status)
  }

  /** Both entry points share validation errors, resource cleanup, and exit statuses. */
  def execute(configuration: => RetrievalConfig): Int = {
    val config = try configuration catch {
      case NonFatal(error) => System.err.println(s"Configuration error: ${error.getMessage}"); return 2
    }
    val downloader = new HttpDownload()
    val verifier = new SparkParquetVerifier(config.master)
    val status = try {
      println(s"Raw directory: ${config.rawDir}")
      println(s"Runtime: Java ${sys.props("java.version")}, Scala ${scala.util.Properties.versionNumberString}, master ${config.master}")
      val results = new RetrievalRunner(downloader, verifier).run(config)
      val passed = results.count(_.success)
      println(s"Summary: $passed/${results.size} months verified, ${results.size - passed} failed")
      if (passed == results.size) 0 else 1
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt(); System.err.println("Retrieval interrupted"); 130
      case NonFatal(error) => System.err.println(s"Retrieval failed: ${error.getMessage}"); 1
    } finally {
      try verifier.close() finally downloader.close()
    }
    status
  }
}
