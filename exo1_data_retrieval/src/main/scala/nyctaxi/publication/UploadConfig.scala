package nyctaxi.publication

import java.net.URI
import java.nio.file.{Path, Paths}
import nyctaxi.retrieval.RetrievalConfig

/** Credentials are deliberately excluded from toString and publication receipts. */
final case class UploadConfig(
  months: Vector[String], rawDir: Path, root: Path, master: String,
  endpoint: URI, accessKey: String, secretKey: String, bucket: String
) {
  override def toString: String = s"UploadConfig($months,$rawDir,$master,$endpoint,$bucket)"
  def redact(message: String): String =
    Vector(accessKey, secretKey).filter(_.nonEmpty).foldLeft(message)((text, key) => text.replace(key, "[redacted]"))
}

object UploadConfig {
  /** Both launch paths share M2.2's month and path validation. */
  def parse(args: Array[String], env: Map[String, String], positional: Boolean = false,
    root: Path = Paths.get(sys.props.getOrElse("nyctaxi.repositoryRoot", ""))): UploadConfig = {
    require(if (positional) args.length <= 2 && !args.exists(_.startsWith("--")) else args.isEmpty,
      "Use upload [months] [rawDir], standalone --help, or the environment-based runMain entry point")
    val overrides = Vector("TAXI_MONTHS", "RAW_DIR").zip(args).toMap
    val values = Map("SPARK_MASTER" -> "local[2]") ++ env ++ overrides
    val local = RetrievalConfig.parse(Array.empty, values, root)
    val endpoint = URI.create(values.getOrElse("S3_ENDPOINT", "http://localhost:9000"))
    require(Set("http", "https").contains(endpoint.getScheme) && endpoint.getHost != null &&
      endpoint.getUserInfo == null && endpoint.getQuery == null && endpoint.getFragment == null &&
      Set("", "/").contains(Option(endpoint.getPath).getOrElse("")), "S3_ENDPOINT must be an HTTP(S) origin without credentials")
    val access = values.getOrElse("S3_ACCESS_KEY", "rustfsadmin")
    val secret = values.getOrElse("S3_SECRET_KEY", "rustfsadmin")
    require(access.trim.nonEmpty && secret.trim.nonEmpty, "S3 credentials must not be empty")
    val bucket = values.getOrElse("S3_BUCKET", "nyc-taxi")
    require(bucket.matches("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]") && !bucket.contains("..") &&
      !bucket.contains(".-") && !bucket.contains("-.") && !bucket.matches("[0-9]+(?:\\.[0-9]+){3}"), "Invalid S3_BUCKET")
    UploadConfig(local.months, local.rawDir, root.toAbsolutePath.normalize(), local.master, endpoint, access, secret, bucket)
  }
}
