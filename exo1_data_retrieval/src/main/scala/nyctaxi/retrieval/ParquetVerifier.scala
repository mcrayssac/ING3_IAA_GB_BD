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

/** Uses a separate local Spark read after HTTP transfer, without transforming source bytes. */
final class SparkParquetVerifier(master: String) extends ParquetVerifier {
  private var session: Option[SparkSession] = None

  private def spark: SparkSession = session.getOrElse {
    val created = SparkSession.builder().appName("tlc-local-retrieval").master(master)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.files.ignoreCorruptFiles", "false")
      .config("spark.sql.files.ignoreMissingFiles", "false")
      .config("spark.sql.parquet.aggregatePushdown", "false")
      .getOrCreate()
    created.sparkContext.setLogLevel("WARN")
    session = Some(created)
    created
  }

  override def verify(path: Path): Verification = {
    val current = spark
    val input = HadoopInputFile.fromPath(new HadoopPath(path.toUri), current.sparkContext.hadoopConfiguration)
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
