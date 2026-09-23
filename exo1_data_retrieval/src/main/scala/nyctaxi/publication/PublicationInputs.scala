package nyctaxi.publication

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.io.{ByteArrayInputStream, InputStream}
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.time.Instant
import nyctaxi.retrieval.{FileDigest, ProvenanceStore, RetrievalConfig, Verification}
import scala.jdk.CollectionConverters._
import scala.util.Using

/** A reopenable source supports bounded-memory retries without changing accepted local bytes. */
final case class Payload(bytes: Long, sha256: String, open: () => InputStream)
object Payload {
  def digest(bytes: Array[Byte]): String = MessageDigest.getInstance("SHA-256")
    .digest(bytes).map(b => f"${b & 0xff}%02x").mkString
  def memory(bytes: Array[Byte]): Payload = Payload(bytes.length, digest(bytes), () => new ByteArrayInputStream(bytes))
  def file(path: Path, size: Long, sha256: String): Payload = {
    require(Files.isRegularFile(path) && Files.size(path) == size && FileDigest.sha256(path) == sha256,
      s"Local size or SHA-256 mismatch: ${path.getFileName}")
    Payload(size, sha256, () => Files.newInputStream(path))
  }
}

final case class PublicationInput(
  key: String, payload: Payload, source: JsonNode, provenanceKey: String,
  provenance: Payload, expected: Option[Verification]
)

/** References are pinned to the M2.1 descriptor, never rediscovered or downloaded during upload. */
final class PublicationInputs(descriptor: Array[Byte] = PublicationInputs.descriptor) {
  private val mapper = new ObjectMapper()
  private val snapshot = mapper.readTree(descriptor)
  require(snapshot.path("formatVersion").asInt() == 1 && snapshot.path("snapshotId").asText() == "20260919T204934Z",
    "Unsupported reference descriptor")
  Instant.parse(snapshot.path("retrievedAt").asText())
  val snapshotId: String = snapshot.path("snapshotId").asText()
  private val referenceFiles = snapshot.path("files").elements().asScala.toVector
  require(referenceFiles.map(_.path("filename").asText()) == Vector("data_dictionary_trip_records_yellow.pdf", "taxi_zone_lookup.csv"),
    "Unexpected reference filenames")

  /** Deferred validation lets one bad input fail independently while later inputs are attempted. */
  def selected(config: UploadConfig): Vector[(String, () => PublicationInput)] = {
    val store = new ProvenanceStore()
    val trips = config.months.map { month =>
      val filename = RetrievalConfig.filename(month)
      filename -> (() => {
        val file = config.rawDir.resolve(filename)
        val saved = store.existing(file, month, RetrievalConfig.source(month).toString)
          .getOrElse(throw new IllegalArgumentException(s"Missing verified local input: $filename"))
        val sidecar = Files.readAllBytes(store.metadata(file))
        val provenance = Payload.memory(sidecar)
        PublicationInput(s"nyc_raw/$filename", Payload.file(file, saved.bytes, saved.sha256), mapper.readTree(sidecar),
          s"nyc_metadata/tlc/trips/${provenance.sha256}/$filename.metadata.json", provenance, Some(saved.verification))
      })
    }
    trips ++ referenceFiles.map { reference =>
      val filename = reference.path("filename").asText()
      filename -> (() => PublicationInput(s"nyc_reference/tlc/$snapshotId/$filename",
        Payload.file(config.root.resolve(s"data/reference/tlc/$snapshotId/$filename"),
          reference.path("bytes").asLong(), reference.path("sha256").asText()), reference,
        s"nyc_metadata/tlc/references/$snapshotId.json", Payload.memory(descriptor), None))
    }
  }
}

object PublicationInputs {
  def descriptor: Array[Byte] = Using.resource(getClass.getResourceAsStream("/tlc-reference-snapshot.json")) { stream =>
    require(stream != null, "Missing reference descriptor")
    stream.readAllBytes()
  }
}
