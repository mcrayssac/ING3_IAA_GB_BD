package nyctaxi.publication

import scala.util.control.NonFatal

object RustFsUpload {
  val help: String = """Usage: upload [months] [rawDir] | --help
    |Alternative: runMain nyctaxi.publication.RustFsUpload [--help]
    |Positional months and rawDir override TAXI_MONTHS and RAW_DIR for upload only.
    |Defaults: all three May-July 2026 months, repository-root data/raw, SPARK_MASTER=local[2].
    |Every run includes reference snapshot 20260919T204934Z from data/reference/tlc/.
    |S3_ENDPOINT=http://localhost:9000, S3_BUCKET=nyc-taxi.
    |S3_ACCESS_KEY and S3_SECRET_KEY default to the documented development credentials.
    |Configuration comes from the process environment. .env is not loaded.
    |Matching objects are verified and reused. Conflicting objects are never overwritten.
    |No downloads or --refresh. Use verified local inputs from retrieve.
    |""".stripMargin

  def main(args: Array[String]): Unit = launch(args, positional = false)

  /** JVM exits occur only at the command boundary, keeping orchestration testable. */
  def launch(args: Array[String], positional: Boolean): Unit = {
    if (args.toVector == Vector("--help")) { println(help); return }
    val code = execute(() => UploadConfig.parse(args, sys.env, positional))
    if (code != 0) System.exit(code)
  }

  def execute(configuration: () => UploadConfig): Int = {
    val config = try configuration() catch {
      // Parser diagnostics contain setting names rather than supplied credentials or URIs.
      case NonFatal(_) => System.err.println("Invalid upload configuration. Run upload --help."); return 2
    }
    var store: ObjectStore = null
    var verifier: RemoteVerifier = null
    try {
      store = new S3aObjectStore(config)
      verifier = new SparkRemoteVerifier(config)
      val report = new PublicationRunner(store, verifier).run(config)
      println(s"Summary: ${report.results.count(_.success)}/${report.results.size} source files verified")
      if (report.success) 0 else 1
    } catch {
      case _: InterruptedException => Thread.currentThread().interrupt(); System.err.println("Publication interrupted"); 130
      case NonFatal(error) =>
        System.err.println("Publication failed: " + config.redact(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))
        1
    } finally {
      try if (verifier != null) verifier.close() finally if (store != null) store.close()
    }
  }
}

/** sbt's short command supplies positional overrides to the same implementation. */
object UploadMain {
  def main(args: Array[String]): Unit = RustFsUpload.launch(args, positional = true)
}
