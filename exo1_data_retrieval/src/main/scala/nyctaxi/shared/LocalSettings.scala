package nyctaxi.shared

import java.nio.file.Path
import nyctaxi.contract.Month

/** Month, RAW_DIR, and local Spark master settings shared by retrieve and upload.
  *
  * Input: merged environment and positional values, and the repository root.
  * Output: selected months, an absolute RAW_DIR, and a local Spark master.
  * Failure: `IllegalArgumentException` naming the invalid setting, before any file or network access.
  */
final case class LocalSettings(months: Vector[Month], rawDir: Path, master: String)

object LocalSettings {
    /** Validates the settings both commands share. A relative RAW_DIR resolves against the root. */
    def parse(values: Map[String, String], root: Path): LocalSettings = {
        val allowed = Month.selected.map(_.value)
        val months =
            values.getOrElse("TAXI_MONTHS", allowed.mkString(",")).split(",", -1).toVector.map(_.trim)
        require(
            months.nonEmpty && months.forall(allowed.contains),
            "TAXI_MONTHS must select 2026-05, 2026-06, or 2026-07 using comma-separated YYYY-MM values"
        )
        require(months.distinct == months, "TAXI_MONTHS must not contain duplicates")
        val raw = values.getOrElse("RAW_DIR", "data/raw")
        require(raw.trim.nonEmpty, "RAW_DIR must not be empty")
        val master = values.getOrElse("SPARK_MASTER", "local[*]")
        require(
            master.matches("local(?:\\[(?:\\*|[1-9][0-9]*)(?:,[1-9][0-9]*)?\\])?"),
            "SPARK_MASTER must use local execution, for example local[2] or local[*]"
        )
        LocalSettings(months.map(Month(_)), root.toAbsolutePath.resolve(raw).normalize(), master)
    }
}
