package nyctaxi.publication

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.nio.file.{Files, Path}
import java.time.{Duration, Instant}
import nyctaxi.contract.{Month, StorageKey, StorageLayout}
import nyctaxi.shared.{
    Digest, Fixtures, Provenance, ProvenanceCodec, ProvenanceFiles, SparkParquetVerifier,
    Verification
}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable

/** Small fixtures exercise publication without TLC downloads or Docker in ordinary tests. */
object PublicationFixtures {
    val policy: StoragePolicy = StoragePolicy(Duration.ofSeconds(1), Duration.ofSeconds(5), 3, 1L)
    def config(root: Path): UploadConfig = UploadConfig.parse(
        Array.empty,
        Map("TAXI_MONTHS" -> "2026-05", "RAW_DIR" -> "raw"),
        root = root
    )

    def parquet(): (Array[Byte], Verification) = Fixtures.directory { root =>
        val spark = SparkSession.builder().master("local[2]").appName("publication-fixtures")
            .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1").getOrCreate()
        val verifier = new SparkParquetVerifier("local[2]")
        try {
            spark.sparkContext.setLogLevel("ERROR")
            spark.range(10).selectExpr("id", "concat('value-', id) as label").coalesce(1)
                .write.option("compression", "uncompressed").parquet(root.resolve("fixture").toString)
            val file = Fixtures.children(root.resolve("fixture")).find(_.toString.endsWith(".parquet")).get
            (Files.readAllBytes(file), verifier.verify(file))
        } finally { verifier.close(); spark.stop() }
    }

    /** Writes accepted file/sidecar pairs directly, as retrieval would have promoted them. */
    def stage(root: Path, bytes: Array[Byte], report: Verification): PublicationInputs = {
        val raw = Files.createDirectories(root.resolve("raw"))
        Month.selected.foreach { month =>
            val file = Files.write(raw.resolve(StorageLayout.tripFilename(month)), bytes)
            val source = StorageLayout.tripSource(month).toString
            Files.write(
                ProvenanceFiles.metadata(file),
                ProvenanceCodec.toBytes(Provenance(
                    month.value,
                    StorageLayout.tripFilename(month),
                    source,
                    source,
                    Instant.now().toString,
                    bytes.length,
                    Digest.sha256(bytes),
                    report
                ))
            )
        }
        val mapper = new ObjectMapper()
        val descriptor = mapper.readTree(PublicationInputs.descriptor)
        val refs = Files.createDirectories(root.resolve("data/reference/tlc/20260919T204934Z"))
        val files = descriptor.path("files")
        for (i <- 0 until files.size()) {
            val node = files.get(i).asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
            val content = s"reference fixture $i".getBytes(java.nio.charset.StandardCharsets.UTF_8)
            Files.write(refs.resolve(node.path("filename").asText()), content)
            node.put("bytes", content.length).put("sha256", Digest.sha256(content))
        }
        new PublicationInputs(mapper.writeValueAsBytes(descriptor))
    }

    /** In-memory bucket keyed by plain strings so assertions stay readable. */
    class MemoryStore extends ObjectStore {
        val entries: mutable.Map[String, Array[Byte]] = mutable.Map.empty
        val writes: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty
        var aborts = 0
        override def ensureBucket(): Unit = ()
        override def stat(key: StorageKey): Option[ObjectInfo] =
            synchronized { entries.get(key.value).map(b => ObjectInfo(b.length, 1L)) }
        override def open(key: StorageKey): InputStream =
            synchronized { new ByteArrayInputStream(entries(key.value)) }
        override def create(key: StorageKey): ObjectWrite = new ObjectWrite {
            private val buffer = new ByteArrayOutputStream()
            override def output: ByteArrayOutputStream = buffer
            override def commit(): Unit = MemoryStore.this.synchronized {
                if (entries.contains(key.value)) throw new java.nio.file.FileAlreadyExistsException(key.value)
                entries(key.value) = buffer.toByteArray
                writes += key.value
            }
            override def abort(): Unit = MemoryStore.this.synchronized { aborts += 1 }
        }
    }
}
