package nyctaxi.publication

import java.net.URI
import java.nio.file.{Path, Paths}
import nyctaxi.contract.Month
import nyctaxi.shared.LocalSettings

/** Validated upload settings for RustFS publication (interfaces I3 and I11, task M2.3).
  *
  * Input: positional overrides, the process environment as a map, and the repository root.
  * Output: months, RAW_DIR, local Spark master, S3 endpoint, credentials, and bucket.
  * Failure: `IllegalArgumentException` before any storage access. Credentials never appear in `toString`.
  */
final case class UploadConfig(
    months: Vector[Month],
    rawDir: Path,
    root: Path,
    master: String,
    endpoint: URI,
    accessKey: String,
    secretKey: String,
    bucket: String
) {
    override def toString: String = s"UploadConfig($months,$rawDir,$master,$endpoint,$bucket)"

    /** Removes both credentials from a message before it is printed or logged. */
    def redact(message: String): String =
        Vector(accessKey, secretKey).filter(_.nonEmpty)
            .foldLeft(message)((text, key) => text.replace(key, "[redacted]"))
}

object UploadConfig {
    /** Both launch paths share the month, RAW_DIR, and master validation of retrieve. */
    def parse(
        args: Array[String],
        env: Map[String, String],
        positional: Boolean = false,
        root: Path = Paths.get(sys.props.getOrElse("nyctaxi.repositoryRoot", ""))
    ): UploadConfig = {
        require(
            if (positional) args.length <= 2 && !args.exists(_.startsWith("--")) else args.isEmpty,
            "Use upload [months] [rawDir], standalone --help, or the environment-based runMain entry point"
        )
        val overrides = Vector("TAXI_MONTHS", "RAW_DIR").zip(args).toMap
        val values = Map("SPARK_MASTER" -> "local[2]") ++ env ++ overrides
        val local = LocalSettings.parse(values, root)
        val (access, secret) = credentials(values)
        UploadConfig(
            local.months,
            local.rawDir,
            root.toAbsolutePath.normalize(),
            local.master,
            endpoint(values),
            access,
            secret,
            bucket(values)
        )
    }

    /** Accepts only an HTTP(S) origin, so credentials can never travel inside the URI. */
    private def endpoint(values: Map[String, String]): URI = {
        val uri = URI.create(values.getOrElse("S3_ENDPOINT", "http://localhost:9000"))
        require(
            Set("http", "https").contains(uri.getScheme) && uri.getHost != null &&
                uri.getUserInfo == null && uri.getQuery == null && uri.getFragment == null &&
                Set("", "/").contains(Option(uri.getPath).getOrElse("")),
            "S3_ENDPOINT must be an HTTP(S) origin without credentials"
        )
        uri
    }

    /** Reads the access and secret keys, rejecting blank values. */
    private def credentials(values: Map[String, String]): (String, String) = {
        // LIMIT: the documented development credentials are the default. Shared RustFS must set both keys.
        val access = values.getOrElse("S3_ACCESS_KEY", "rustfsadmin")
        val secret = values.getOrElse("S3_SECRET_KEY", "rustfsadmin")
        require(access.trim.nonEmpty && secret.trim.nonEmpty, "S3 credentials must not be empty")
        (access, secret)
    }

    /** Applies the S3 bucket naming rules, including the IP-address and dot-dash exclusions. */
    private def bucket(values: Map[String, String]): String = {
        val name = values.getOrElse("S3_BUCKET", "nyc-taxi")
        require(
            name.matches("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]") && !name.contains("..") &&
                !name.contains(".-") && !name.contains("-.") && !name.matches("[0-9]+(?:\\.[0-9]+){3}"),
            "Invalid S3_BUCKET"
        )
        name
    }
}
