package nyctaxi.retrieval

import java.nio.file.Path
import java.time.Instant
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.{Row, SparkSession}
import scala.util.Using

/** Observations describe the original file, not a cleaned-data contract. */
final case class Verification(
  parquetSchema: String, sparkSchema: String, rowCount: Long, verifiedAt: String,
  javaVersion: String, scalaVersion: String, sparkVersion: String
)

trait ParquetVerifier extends AutoCloseable {
  def verify(path: Path): Verification
  override def close(): Unit = ()
}

object SparkParquetVerifier {
  /** Force nested values as well as primitive columns without collecting rows on the driver. */
  private def consume(value: Any): Unit = value match {
    case row: Row => row.toSeq.foreach(consume)
    case values: scala.collection.Map[_, _] => values.foreach { case (key, item) => consume(key); consume(item) }
    case values: Iterable[_] => values.foreach(consume)
    case values: Array[_] => values.foreach(consume)
    case _ => ()
  }

  def countDecoded(rows: Iterator[Row]): Iterator[Long] = {
    var count = 0L
    rows.foreach { row => consume(row); count += 1L }
    Iterator.single(count)
  }
}

/** Completely decodes local or S3A sources without transforming their bytes. */
final class SparkParquetVerifier(master: String, hadoopSettings: Map[String, String] = Map.empty) extends ParquetVerifier {
  private var session: Option[SparkSession] = None

  private def spark: SparkSession = session.getOrElse {
    val builder = SparkSession.builder().appName("tlc-source-verification").master(master)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.files.ignoreCorruptFiles", "false")
      .config("spark.sql.files.ignoreMissingFiles", "false")
      .config("spark.sql.parquet.aggregatePushdown", "false")
    hadoopSettings.foreach { case (key, value) => builder.config("spark.hadoop." + key, value) }
    val created = builder.getOrCreate()
    created.sparkContext.setLogLevel("WARN")
    session = Some(created)
    created
  }

  override def verify(path: Path): Verification = verify(new HadoopPath(path.toUri))

  /** The same complete projection verifies local files and individual S3A objects. */
  def verify(path: HadoopPath): Verification = {
    val current = spark
    hadoopSettings.foreach { case (key, value) => current.sparkContext.hadoopConfiguration.set(key, value) }
    val input = HadoopInputFile.fromPath(path, current.sparkContext.hadoopConfiguration)
    val physical = Using.resource(ParquetFileReader.open(input))(_.getFooter.getFileMetaData.getSchema.toString)
    val data = current.read.option("ignoreCorruptFiles", "false").option("ignoreMissingFiles", "false")
      .parquet(path.toUri.toString)
    // RDD conversion keeps the complete projection. A SQL count could skip column decoding.
    val count = data.rdd.mapPartitions(SparkParquetVerifier.countDecoded).fold(0L)(_ + _)
    Verification(physical, data.schema.json, count, Instant.now().toString,
      sys.props("java.version"), scala.util.Properties.versionNumberString, current.version)
  }

  override def close(): Unit = {
    session.foreach(_.stop())
    session = None
  }
}
