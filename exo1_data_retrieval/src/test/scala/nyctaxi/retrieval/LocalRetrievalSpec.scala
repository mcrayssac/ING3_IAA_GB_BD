package nyctaxi.retrieval

import java.nio.file.{Files, Path}
import java.time.Duration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import scala.util.Using

/** Exercises the real Spark verifier and provenance lifecycle with tiny local Parquet fixtures. */
class LocalRetrievalSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _
  private var bytes: Array[Byte] = _
  private val policy = DownloadPolicy(Duration.ofSeconds(1), Duration.ofSeconds(2), 1, 1L)

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder().appName("retrieval-fixtures").master("local[2]")
      .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    Fixtures.directory { directory =>
      val destination = directory.resolve("fixture")
      spark.range(10).selectExpr("id", "concat('value-', id) as label", "cast(null as double) as missing")
        .coalesce(1).write.option("compression", "uncompressed").parquet(destination.toString)
      bytes = Files.readAllBytes(Fixtures.children(destination).find(_.getFileName.toString.endsWith(".parquet")).get)
    }
  }

  override def afterAll(): Unit = {
    try if (spark != null) spark.stop() finally super.afterAll()
  }

  private def config(directory: Path, refresh: Boolean = false): RetrievalConfig =
    RetrievalConfig(Vector("2026-05"), directory, "local[2]", refresh)

  test("configuration resolves raw paths from the root and rejects invalid input before execution") {
    Fixtures.directory { root =>
      val defaults = RetrievalConfig.parse(Array.empty, Map.empty, root)
      assert(defaults.rawDir == root.resolve("data/raw") && defaults.months == RetrievalConfig.selectedMonths)
      val subset = RetrievalConfig.parse(Array("--refresh"), Map("TAXI_MONTHS" -> "2026-07", "SPARK_MASTER" -> "local[2]"), root)
      assert(subset.months == Vector("2026-07") && subset.refresh)
      for (months <- Vector("", "2026-05,", "2026-05,2026-05", "2026-5", "2026-08"))
        intercept[IllegalArgumentException](RetrievalConfig.parse(Array.empty, Map("TAXI_MONTHS" -> months), root))
      for (master <- Vector("spark://spark-master:7077", "local-cluster[2,1,1024]", "local[0]", ""))
        intercept[IllegalArgumentException](RetrievalConfig.parse(Array.empty, Map("SPARK_MASTER" -> master), root))
      intercept[IllegalArgumentException](RetrievalConfig.parse(Array("--unknown"), Map.empty, root))
      intercept[IllegalArgumentException](RetrievalConfig.parse(Array.empty, Map("RAW_DIR" -> ""), root))
      assert(Fixtures.children(root).isEmpty)
    }
  }

  test("download, fully decode, reuse without HTTP, and refresh with metadata history") {
    Fixtures.directory { directory =>
      Fixtures.server((exchange, _) => Fixtures.respond(exchange, 200, bytes)) { (base, requests) =>
        Using.resource(new HttpDownload(policy)) { downloader =>
          val verifier = new SparkParquetVerifier("local[2]")
          val store = new ProvenanceStore()
          val runner = new RetrievalRunner(downloader, verifier, store, month => base.resolve(month), _ => ())
          val file = directory.resolve(RetrievalConfig.filename("2026-05"))
          assert(runner.run(config(directory)).forall(_.success))
          val original = Files.readAllBytes(store.metadata(file))
          val saved = store.existing(file, "2026-05", base.resolve("2026-05").toString).get
          assert(saved.verification.rowCount == 10L && saved.verification.parquetSchema.contains("label"))
          assert(saved.verification.sparkSchema.contains("missing") && Files.readAllBytes(file).sameElements(bytes))
          assert(runner.run(config(directory)).forall(_.success) && requests.get() == 1)
          assert(Files.readAllBytes(store.metadata(file)).sameElements(original))
          assert(runner.run(config(directory, refresh = true)).forall(_.success) && requests.get() == 2)
          val history = Fixtures.children(directory.resolve(".history").resolve(file.getFileName))
          assert(history.size == 1 && Files.readAllBytes(history.head).sameElements(original))
          assert(!Files.exists(store.pending(file)))
        }
      }
    }
  }

  test("missing metadata, changed bytes, and interrupted promotion fail without HTTP or overwrites") {
    for (damage <- Vector("metadata", "invalid-metadata", "bytes", "marker", "file")) Fixtures.directory { directory =>
      Fixtures.server((exchange, _) => Fixtures.respond(exchange, 200, bytes)) { (base, requests) =>
        Using.resource(new HttpDownload(policy)) { downloader =>
          val store = new ProvenanceStore()
          val runner = new RetrievalRunner(downloader, new SparkParquetVerifier("local[2]"), store, month => base.resolve(month), _ => ())
          val file = directory.resolve(RetrievalConfig.filename("2026-05"))
          assert(runner.run(config(directory)).forall(_.success))
          damage match {
            case "metadata" => Files.delete(store.metadata(file))
            case "invalid-metadata" => Files.writeString(store.metadata(file), "{\"formatVersion\":2}")
            case "bytes" => val changed = bytes.clone(); changed(10) = (changed(10) ^ 1).toByte; Files.write(file, changed)
            case "marker" => Files.writeString(store.pending(file), "interrupted")
            case "file" => Files.delete(file)
          }
          assert(runner.run(config(directory)).forall(!_.success) && requests.get() == 1)
        }
      }
    }
  }

  test("a failed refresh preserves the accepted pair and later months still run") {
    Fixtures.directory { directory =>
      Fixtures.server { (exchange, number) =>
        Fixtures.respond(exchange, 200, if (number == 2) "not parquet".getBytes else bytes)
      } { (base, requests) =>
        Using.resource(new HttpDownload(policy)) { downloader =>
          val store = new ProvenanceStore()
          val runner = new RetrievalRunner(downloader, new SparkParquetVerifier("local[2]"), store, month => base.resolve(month), _ => ())
          val file = directory.resolve(RetrievalConfig.filename("2026-05"))
          assert(runner.run(config(directory)).forall(_.success))
          val original = Files.readAllBytes(store.metadata(file))
          val results = runner.run(config(directory, refresh = true).copy(months = Vector("2026-05", "2026-06")))
          assert(results.map(_.success) == Vector(false, true) && requests.get() == 3)
          assert(Files.readAllBytes(file).sameElements(bytes))
          assert(Files.readAllBytes(store.metadata(file)).sameElements(original))
          assert(!Files.exists(store.pending(file)))
          assert(!Fixtures.children(directory).exists(_.getFileName.toString.endsWith(".part")))
        }
      }
    }
  }

  test("full decoding rejects a corrupt column chunk whose footer is still readable") {
    Fixtures.directory { directory =>
      val file = Files.write(directory.resolve("corrupt.parquet"), bytes)
      val input = HadoopInputFile.fromPath(new HadoopPath(file.toUri), spark.sparkContext.hadoopConfiguration)
      val chunk = Using.resource(ParquetFileReader.open(input))(_.getFooter.getBlocks.get(0).getColumns.get(1))
      val corrupted = bytes.clone()
      java.util.Arrays.fill(corrupted, chunk.getStartingPos.toInt, (chunk.getStartingPos + chunk.getTotalSize).toInt, 0.toByte)
      Files.write(file, corrupted)
      assert(Using.resource(ParquetFileReader.open(input))(_.getFooter.getBlocks.get(0).getRowCount) == 10L)
      intercept[Exception](new SparkParquetVerifier("local[2]").verify(file))
    }
  }

  test("HTTP failure during refresh preserves the accepted pair") {
    Fixtures.directory { directory =>
      Fixtures.server((exchange, number) => Fixtures.respond(exchange, if (number == 1) 200 else 503, bytes)) { (base, _) =>
        Using.resource(new HttpDownload(policy)) { downloader =>
          val store = new ProvenanceStore()
          val runner = new RetrievalRunner(downloader, new SparkParquetVerifier("local[2]"), store, month => base.resolve(month), _ => ())
          val file = directory.resolve(RetrievalConfig.filename("2026-05"))
          assert(runner.run(config(directory)).forall(_.success))
          val original = Files.readAllBytes(store.metadata(file))
          assert(runner.run(config(directory, refresh = true)).forall(!_.success))
          assert(Files.readAllBytes(file).sameElements(bytes))
          assert(Files.readAllBytes(store.metadata(file)).sameElements(original))
        }
      }
    }
  }

  test("verification interruption aborts the run rather than continuing with the next month") {
    Fixtures.directory { directory =>
      Fixtures.server((exchange, _) => Fixtures.respond(exchange, 200, bytes)) { (base, requests) =>
        Using.resource(new HttpDownload(policy)) { downloader =>
          val interrupted = new ParquetVerifier {
            override def verify(path: Path): Verification = throw new InterruptedException("cancelled")
          }
          val runner = new RetrievalRunner(downloader, interrupted, source = month => base.resolve(month), log = _ => ())
          intercept[InterruptedException](runner.run(config(directory).copy(months = RetrievalConfig.selectedMonths)))
          assert(requests.get() == 1)
          assert(Fixtures.children(directory).forall(_.getFileName.toString == ".retrieval.lock"))
        }
      }
    }
  }
}
