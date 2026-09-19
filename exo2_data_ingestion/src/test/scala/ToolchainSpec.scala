import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

/** Proves the ingestion module can execute Spark using the shared toolchain. */
class ToolchainSpec extends AnyFunSuite {
  test("JDK 21 and Scala 2.13.18 execute Spark 4.2.0 locally") {
    val javaVersion = sys.props("java.version")
    val scalaVersion = scala.util.Properties.versionNumberString
    info(s"Test JVM: Java $javaVersion, Scala $scalaVersion")
    assert(sys.props("java.specification.version") == "21", "Select JDK 21 through JAVA_HOME")
    assert(scalaVersion == "2.13.18")

    val spark = SparkSession.builder()
      .appName("ingestion-toolchain-smoke")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      // Keep this local check independent of hostname resolution and external services.
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    try {
      spark.sparkContext.setLogLevel("WARN")
      info(s"Spark ${spark.version}, master ${spark.sparkContext.master}")
      assert(spark.version == "4.2.0")
      assert(spark.range(10).count() == 10L)
    } finally {
      // Release Spark threads and ports even when an assertion fails.
      spark.stop()
    }
  }
}
