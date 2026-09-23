package nyctaxi.publication

import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.channels.FileChannel
import java.nio.file.{Files, StandardOpenOption}
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID
import nyctaxi.retrieval.{SparkParquetVerifier, Verification}
import org.apache.hadoop.fs.{Path => HadoopPath}
import scala.util.Using
import scala.util.control.NonFatal

trait RemoteVerifier extends AutoCloseable {
  def verify(key: String): Verification
  override def close(): Unit = ()
}

final class SparkRemoteVerifier(config: UploadConfig, policy: StoragePolicy = StoragePolicy()) extends RemoteVerifier {
  private val verifier = new SparkParquetVerifier(config.master, S3aObjectStore.settings(config, policy))
  override def verify(key: String): Verification = verifier.verify(new HadoopPath(s"s3a://${config.bucket}/$key"))
  override def close(): Unit = verifier.close()
}

final case class ItemResult(name: String, success: Boolean, message: String)
final case class PublicationReport(results: Vector[ItemResult], receipt: Option[String]) {
  def success: Boolean = results.forall(_.success) && receipt.nonEmpty
}

/** Object acceptance requires independent remote hashing, not ETags or advertised metadata. */
final class ObjectPublisher(store: ObjectStore, operations: StorageOperations) {
  private def check(key: String, payload: Payload, scope: OperationScope): Unit = {
    val info = store.stat(key).getOrElse(throw new java.io.IOException(s"Missing remote object: $key"))
    if (info.bytes != payload.bytes) throw new PublicationConflict(s"Remote size conflict: $key")
    Using.resource(store.open(key)) { input =>
      scope.onCancel(() => input.close())
      val observed = StreamDigest.read(input, scope)
      if (observed != ((payload.bytes, payload.sha256))) throw new PublicationConflict(s"Remote SHA-256 conflict: $key")
    }
  }

  /** Retrying starts by checking the destination, covering an accepted PUT with a lost response. */
  def publish(key: String, payload: Payload): String = operations.run { scope =>
    if (store.stat(key).nonEmpty) { check(key, payload, scope); "reused" }
    else {
      val pending = store.create(key)
      var committed = false
      scope.onCancel(() => pending.abort())
      try {
        Using.resource(payload.open()) { input =>
          scope.onCancel(() => input.close())
          val observed = StreamDigest.read(input, scope, Some(pending.output))
          require(observed == ((payload.bytes, payload.sha256)), s"Source changed during upload: $key")
        }
        scope.check()
        pending.commit()
        committed = true
      } finally if (!committed) pending.abort()
      check(key, payload, scope)
      "uploaded"
    }
  }
}

/** Completed objects survive a later failure. Only a completely verified run receives a receipt. */
final class PublicationRunner(
  store: ObjectStore, verifier: RemoteVerifier, inputs: PublicationInputs = new PublicationInputs(),
  policy: StoragePolicy = StoragePolicy(), log: String => Unit = println
) {
  private val mapper = new ObjectMapper()
  private val operations = new StorageOperations(policy)
  private val publisher = new ObjectPublisher(store, operations)

  def run(config: UploadConfig): PublicationReport = {
    require(Files.isDirectory(config.rawDir), "RAW_DIR must contain verified local downloads")
    Using.resource(FileChannel.open(config.rawDir.resolve(".retrieval.lock"), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) { channel =>
      val lock = channel.tryLock()
      require(lock != null, "Another retrieval or publication process is using RAW_DIR")
      try publishAll(config) finally lock.release()
    }
  }

  private def publishAll(config: UploadConfig): PublicationReport = {
    operations.run(_ => store.ensureBucket())
    val receipt = mapper.createObjectNode().put("formatVersion", 1).put("bucket", config.bucket)
      .put("endpoint", config.endpoint.toString).put("snapshotId", inputs.snapshotId)
    val months = receipt.putArray("months")
    config.months.foreach(months.add)
    val objects = receipt.putArray("objects")
    val results = inputs.selected(config).map { case (name, load) =>
      if (Thread.currentThread().isInterrupted) throw new InterruptedException("Publication interrupted")
      val result = try {
        val input = load()
        val action = publisher.publish(input.key, input.payload)
        val observed = input.expected.map { expected =>
          val actual = operations.run { scope =>
            scope.onCancel(() => verifier.close())
            val verified = verifier.verify(input.key)
            scope.check()
            verified
          }
          require(actual.rowCount == expected.rowCount && actual.parquetSchema == expected.parquetSchema &&
            actual.sparkSchema == expected.sparkSchema, s"Remote Parquet verification differs from local provenance: $name")
          actual
        }
        val provenanceAction = publisher.publish(input.provenanceKey, input.provenance)
        val entry = objects.addObject().put("key", input.key).put("bytes", input.payload.bytes)
          .put("sha256", input.payload.sha256).put("action", action).put("verifiedAt", Instant.now().toString)
        entry.set[com.fasterxml.jackson.databind.JsonNode]("source", input.source)
        entry.putObject("provenance").put("key", input.provenanceKey).put("bytes", input.provenance.bytes)
          .put("sha256", input.provenance.sha256).put("action", provenanceAction)
        observed.foreach { checked =>
          val verification = entry.putObject("verification").put("rowCount", checked.rowCount)
            .put("parquetSchema", checked.parquetSchema).put("verifiedAt", checked.verifiedAt)
          verification.set[com.fasterxml.jackson.databind.JsonNode]("sparkSchema", mapper.readTree(checked.sparkSchema))
          verification.putObject("versions").put("java", checked.javaVersion).put("scala", checked.scalaVersion).put("spark", checked.sparkVersion)
        }
        ItemResult(name, true, s"$action, remote SHA-256 verified" + observed.map(v => s", ${v.rowCount} rows fully decoded").getOrElse(""))
      } catch {
        case error if StorageFailures.denied(error) => throw new StorageAccessFailure
        case NonFatal(error) => ItemResult(name, false, config.redact(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))
      }
      log(s"$name: ${if (result.success) "OK" else "FAILED"} ${result.message}")
      result
    }
    if (results.exists(!_.success)) return PublicationReport(results, None)
    val now = Instant.now()
    val id = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC).format(now) + "-" + UUID.randomUUID()
    receipt.put("publicationId", id).put("completedAt", now.toString)
    receipt.putObject("versions").put("java", sys.props("java.version"))
      .put("scala", scala.util.Properties.versionNumberString).put("spark", org.apache.spark.SPARK_VERSION)
    val key = s"nyc_metadata/tlc/publications/$id.json"
    publisher.publish(key, Payload.memory(mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(receipt)))
    log(s"Verified publication receipt: s3a://${config.bucket}/$key")
    PublicationReport(results, Some(key))
  }
}
