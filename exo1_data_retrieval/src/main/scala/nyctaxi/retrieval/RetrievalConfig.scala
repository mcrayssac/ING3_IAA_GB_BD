package nyctaxi.retrieval

import java.net.URI
import java.nio.file.{Path, Paths}

/** The M2.1 source selection and local execution settings. No dotenv loading occurs. */
final case class RetrievalConfig(months: Vector[String], rawDir: Path, master: String, refresh: Boolean)

object RetrievalConfig {
  val selectedMonths: Vector[String] = Vector("2026-05", "2026-06", "2026-07")

  /** Retain the catalog's original filename and official HTTPS location. */
  def filename(month: String): String = s"yellow_tripdata_$month.parquet"
  def source(month: String): URI =
    URI.create(s"https://d37ci6vzurychx.cloudfront.net/trip-data/${filename(month)}")

  /** Reject configuration errors before creating files or contacting TLC. */
  def parse(args: Array[String], env: Map[String, String],
    root: Path = Paths.get(sys.props.getOrElse("nyctaxi.repositoryRoot", ""))): RetrievalConfig = {
    require(args.isEmpty || args.toVector == Vector("--refresh"), "Use --refresh, --help, or no arguments")
    val months = env.getOrElse("TAXI_MONTHS", selectedMonths.mkString(","))
      .split(",", -1).toVector.map(_.trim)
    require(months.nonEmpty && months.forall(selectedMonths.contains),
      "TAXI_MONTHS must select 2026-05, 2026-06, or 2026-07 using comma-separated YYYY-MM values")
    require(months.distinct == months, "TAXI_MONTHS must not contain duplicates")
    val raw = env.getOrElse("RAW_DIR", "data/raw")
    require(raw.trim.nonEmpty, "RAW_DIR must not be empty")
    val master = env.getOrElse("SPARK_MASTER", "local[*]")
    require(master.matches("local(?:\\[(?:\\*|[1-9][0-9]*)(?:,[1-9][0-9]*)?\\])?"),
      "SPARK_MASTER must use local execution, for example local[2] or local[*]")
    RetrievalConfig(months, root.toAbsolutePath.resolve(raw).normalize(), master, args.nonEmpty)
  }
}
