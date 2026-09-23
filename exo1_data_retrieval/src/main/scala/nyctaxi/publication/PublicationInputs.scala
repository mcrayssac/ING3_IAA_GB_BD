package nyctaxi.publication

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.io.{ByteArrayInputStream, InputStream}
import java.nio.file.{Files, Path}
import java.time.Instant
import nyctaxi.contract.{Month, StorageKey, StorageLayout}
import nyctaxi.shared.{Digest, ProvenanceFiles, Verification}
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Local sources selected for publication (interfaces I2, I3, and I11, task M2.3).
  *
  * Input: accepted trip file and sidecar pairs in RAW_DIR, and the staged reference snapshot files.
  * Output: one deferred `PublicationInput` per source, with its object keys, payloads, and expected decoding.
  * Failure: loading an input throws when its local bytes or provenance do not match.
  *   Other inputs are unaffected.
  */
final case class Payload(bytes: Long, sha256: String, open: () => InputStream)

object Payload {
  /** Small content such as sidecars, descriptors, and receipts. */
  def memory(bytes: Array[Byte]): Payload =
    Payload(bytes.length, Digest.sha256(bytes), () => new ByteArrayInputStream(bytes))

  /** A reopenable file, checked once so retries never publish bytes that changed on disk. */
  def file(path: Path, size: Long, sha256: String): Payload = {
    require(Files.isRegularFile(path) && Files.size(path) == size && Digest.sha256(path) == sha256,
      s"Local size or SHA-256 mismatch: ${path.getFileName}")
    Payload(size, sha256, () => Files.newInputStream(path))
  }
}

final case class PublicationInput(
  key: StorageKey, payload: Payload, source: JsonNode, provenanceKey: StorageKey,
  provenance: Payload, expected: Option[Verification]
)

/** References are pinned to the M2.1 descriptor, never rediscovered or downloaded during upload. */
final class PublicationInputs(descriptor: Array[Byte] = PublicationInputs.descriptor) {
  private val mapper = new ObjectMapper()
  private val snapshot = mapper.readTree(descriptor)
  // LIMIT: reference snapshot pinned to 20260919T204934Z. A new snapshot needs a new descriptor and release.
  require(snapshot.path("formatVersion").asInt() == 1 &&
    snapshot.path("snapshotId").asText() == "20260919T204934Z", "Unsupported reference descriptor")
  Instant.parse(snapshot.path("retrievedAt").asText())
  val snapshotId: String = snapshot.path("snapshotId").asText()
  private val referenceFiles = snapshot.path("files").elements().asScala.toVector
  private val expectedReferences = Vector("data_dictionary_trip_records_yellow.pdf", "taxi_zone_lookup.csv")
  require(referenceFiles.map(_.path("filename").asText()) == expectedReferences,
    "Unexpected reference filenames")

  /** Deferred validation lets one bad input fail independently while later inputs are attempted. */
  def selected(config: UploadConfig): Vector[(String, () => PublicationInput)] =
    config.months.map(month => tripInput(config, month)) ++
      referenceFiles.map(reference => referenceInput(config, reference))

  /** A trip file with its accepted sidecar, published under a key derived from the sidecar checksum. */
  private def tripInput(config: UploadConfig, month: Month): (String, () => PublicationInput) = {
    val filename = StorageLayout.tripFilename(month)
    filename -> (() => {
      val file = config.rawDir.resolve(filename)
      val saved = ProvenanceFiles.existing(file, month, StorageLayout.tripSource(month).toString)
        .getOrElse(throw new IllegalArgumentException(s"Missing verified local input: $filename"))
      val sidecar = Files.readAllBytes(ProvenanceFiles.metadata(file))
      val provenance = Payload.memory(sidecar)
      PublicationInput(StorageLayout.rawTrip(month), Payload.file(file, saved.bytes, saved.sha256),
        mapper.readTree(sidecar), StorageLayout.tripProvenance(month, provenance.sha256), provenance,
        Some(saved.verification))
    })
  }

  /** A reference file checked against the descriptor, which is published as its provenance. */
  private def referenceInput(config: UploadConfig, reference: JsonNode): (String, () => PublicationInput) = {
    val filename = reference.path("filename").asText()
    filename -> (() => PublicationInput(StorageLayout.reference(snapshotId, filename),
      Payload.file(StorageLayout.localReference(config.root, snapshotId, filename),
        reference.path("bytes").asLong(), reference.path("sha256").asText()), reference,
      StorageLayout.referenceDescriptor(snapshotId), Payload.memory(descriptor), None))
  }
}

object PublicationInputs {
  /** The machine-readable M2.1 reference descriptor bundled with the application. */
  def descriptor: Array[Byte] =
    Using.resource(getClass.getResourceAsStream("/tlc-reference-snapshot.json")) { stream =>
      require(stream != null, "Missing reference descriptor")
      stream.readAllBytes()
    }
}
