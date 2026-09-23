package nyctaxi.retrieval

import java.nio.file.{Path, Paths}
import nyctaxi.contract.Month
import nyctaxi.shared.LocalSettings

/** The M2.1 source selection and local execution settings. No dotenv loading occurs. */
final case class RetrievalConfig(months: Vector[Month], rawDir: Path, master: String, refresh: Boolean)

object RetrievalConfig {
  /** Reject configuration errors before creating files or contacting TLC. */
  def parse(args: Array[String], env: Map[String, String],
    root: Path = Paths.get(sys.props.getOrElse("nyctaxi.repositoryRoot", ""))): RetrievalConfig = {
    require(args.isEmpty || args.toVector == Vector("--refresh"), "Use --refresh, --help, or no arguments")
    val local = LocalSettings.parse(env, root)
    RetrievalConfig(local.months, local.rawDir, local.master, args.nonEmpty)
  }
}
