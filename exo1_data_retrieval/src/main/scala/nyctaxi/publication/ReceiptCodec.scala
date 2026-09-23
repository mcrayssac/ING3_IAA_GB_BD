package nyctaxi.publication

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import java.net.URI
import java.time.Instant
import nyctaxi.contract.{Month, StorageKey}
import nyctaxi.shared.{RuntimeVersions, Verification}

/** Provenance object published next to one accepted source, and how it was accepted. */
final case class ProvenanceEntry(key: StorageKey, bytes: Long, sha256: String, action: String)

/** One accepted source object, its provenance, and its remote decoding when it is Parquet. */
final case class ReceiptEntry(
    key: StorageKey,
    bytes: Long,
    sha256: String,
    action: String,
    verifiedAt: String,
    source: JsonNode,
    provenance: ProvenanceEntry,
    verification: Option[Verification]
)

/** Everything recorded about one fully verified publication run. */
final case class Receipt(
    bucket: String,
    endpoint: URI,
    snapshotId: String,
    months: Vector[Month],
    objects: Vector[ReceiptEntry],
    publicationId: String,
    completedAt: Instant,
    versions: RuntimeVersions
)

/** Publication receipt of a fully verified run, format version 1 (interface I3, task M2.3).
  *
  * Input: a `Receipt` built by `PublicationRunner` from accepted items.
  * Output: pretty-printed JSON. Field names and their order are part of the published format.
  * Failure: none beyond Jackson errors on malformed Spark schema JSON.
  */
object ReceiptCodec {
    private val mapper = new ObjectMapper()

    /** Serializes a receipt with the fields and order of format version 1. */
    def toJson(receipt: Receipt): Array[Byte] = {
        val root = mapper.createObjectNode().put("formatVersion", 1).put("bucket", receipt.bucket)
            .put("endpoint", receipt.endpoint.toString).put("snapshotId", receipt.snapshotId)
        val months = root.putArray("months")
        receipt.months.foreach(month => months.add(month.value))
        val objects = root.putArray("objects")
        receipt.objects.foreach(entry => writeEntry(objects.addObject(), entry))
        root.put("publicationId", receipt.publicationId).put("completedAt", receipt.completedAt.toString)
        root.putObject("versions").put("java", receipt.versions.java).put("scala", receipt.versions.scala)
            .put("spark", receipt.versions.spark)
        mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(root)
    }

    /** One accepted object, its provenance object, and its remote decoding when it is Parquet. */
    private def writeEntry(node: ObjectNode, entry: ReceiptEntry): Unit = {
        node.put("key", entry.key.value).put("bytes", entry.bytes).put("sha256", entry.sha256)
            .put("action", entry.action).put("verifiedAt", entry.verifiedAt)
        node.set[JsonNode]("source", entry.source)
        node.putObject("provenance")
            .put("key", entry.provenance.key.value)
            .put("bytes", entry.provenance.bytes)
            .put("sha256", entry.provenance.sha256)
            .put("action", entry.provenance.action)
        entry.verification.foreach { checked =>
            val verification = node.putObject("verification").put("rowCount", checked.rowCount)
                .put("parquetSchema", checked.parquetSchema).put("verifiedAt", checked.verifiedAt)
            verification.set[JsonNode]("sparkSchema", mapper.readTree(checked.sparkSchema))
            verification.putObject("versions")
                .put("java", checked.javaVersion)
                .put("scala", checked.scalaVersion)
                .put("spark", checked.sparkVersion)
        }
    }
}
