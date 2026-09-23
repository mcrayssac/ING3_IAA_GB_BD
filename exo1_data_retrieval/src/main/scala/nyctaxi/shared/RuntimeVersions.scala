package nyctaxi.shared

/** Java, Scala, and Spark versions recorded in provenance, receipts, and run logs. */
final case class RuntimeVersions(java: String, scala: String, spark: String)

object RuntimeVersions {
  /** Versions of the running JVM and of the Spark library on the classpath. */
  def current: RuntimeVersions = RuntimeVersions(sys.props("java.version"),
    scala.util.Properties.versionNumberString, org.apache.spark.SPARK_VERSION)
}
