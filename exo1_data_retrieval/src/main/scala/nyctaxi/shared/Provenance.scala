package nyctaxi.shared

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import java.io.IOException
import java.nio.file.{Files, Path}
import java.time.Instant
import nyctaxi.contract.Month

/** Local provenance of an accepted trip file, format version 1 (interface I2, task M2.2).
  *
  * Input: a trip file in RAW_DIR and its adjacent `.metadata.json` sidecar.
  * Output: the validated `Provenance` of a matching pair, or its version 1 JSON bytes.
  * Failure: an incomplete pair, a `.pending` marker, invalid metadata, or changed bytes throw with an
  *   actionable message. Nothing is created or repaired implicitly.
  */
final case class Provenance(
    month: String,
    filename: String,
    sourceUrl: String,
    finalUrl: String,
    retrievedAt: String,
    bytes: Long,
    sha256: String,
    verification: Verification
)

/** Version 1 JSON form of provenance sidecars. */
object ProvenanceCodec {
    private val mapper = new ObjectMapper()

    private def text(node: JsonNode, key: String): String = {
        val value = node.path(key)
        require(value.isTextual && value.asText().nonEmpty, s"Missing or invalid metadata field: $key")
        value.asText()
    }

    private def number(node: JsonNode, key: String): Long = {
        val value = node.path(key)
        require(value.isIntegralNumber && value.canConvertToLong, s"Invalid metadata number: $key")
        value.asLong()
    }

    /** Reads and validates a sidecar, rejecting unsupported versions and malformed fields. */
    def read(path: Path): Provenance = {
        val node = mapper.readTree(path.toFile)
        require(
            node != null && node.isObject && number(node, "formatVersion") == 1L,
            "Unsupported metadata format"
        )
        val checks = node.path("verification")
        val versions = checks.path("versions")
        val schema = checks.path("sparkSchema")
        require(schema.isObject, "Missing Spark schema")
        val report = Verification(
            text(checks, "parquetSchema"),
            schema.toString,
            number(checks, "rowCount"),
            text(checks, "verifiedAt"),
            text(versions, "java"),
            text(versions, "scala"),
            text(versions, "spark")
        )
        val result = Provenance(
            text(node, "month"),
            text(node, "filename"),
            text(node, "sourceUrl"),
            text(node, "finalUrl"),
            text(node, "retrievedAt"),
            number(node, "bytes"),
            text(node, "sha256"),
            report
        )
        require(
            result.bytes > 0 && report.rowCount >= 0 && result.sha256.matches("[0-9a-f]{64}"),
            "Invalid size, count, or checksum in metadata"
        )
        Instant.parse(result.retrievedAt)
        Instant.parse(report.verifiedAt)
        result
    }

    /** Serializes provenance as pretty-printed version 1 JSON. */
    def toBytes(saved: Provenance): Array[Byte] = {
        val node = mapper.createObjectNode().put("formatVersion", 1)
            .put("month", saved.month).put("filename", saved.filename).put("sourceUrl", saved.sourceUrl)
            .put("finalUrl", saved.finalUrl).put("retrievedAt", saved.retrievedAt)
            .put("bytes", saved.bytes).put("sha256", saved.sha256)
        val report = saved.verification
        val checks = node.putObject("verification").put("verifiedAt", report.verifiedAt)
            .put("parquetSchema", report.parquetSchema).put("rowCount", report.rowCount)
        checks.set[JsonNode]("sparkSchema", mapper.readTree(report.sparkSchema))
        checks.putObject("versions").put("java", report.javaVersion).put("scala", report.scalaVersion)
            .put("spark", report.sparkVersion)
        mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(node)
    }
}

/** Sidecar locations and read-only acceptance of existing file/sidecar pairs. */
object ProvenanceFiles {
    def metadata(file: Path): Path = file.resolveSibling(file.getFileName.toString + ".metadata.json")
    def pending(file: Path): Path = file.resolveSibling(file.getFileName.toString + ".pending")

    /** Returns the provenance of a complete matching pair, or `None` when neither file exists. */
    def existing(file: Path, month: Month, sourceUrl: String): Option[Provenance] = {
        val sidecar = metadata(file)
        if (Files.exists(pending(file)))
            throw new IOException("Interrupted promotion detected. Inspect files and use --refresh")
        if (!Files.exists(file) && !Files.exists(sidecar)) return None
        if (!Files.isRegularFile(file) || !Files.isRegularFile(sidecar))
            throw new IOException("Incomplete file/metadata pair. Inspect files and use --refresh")
        val saved = ProvenanceCodec.read(sidecar)
        require(
            saved.month == month.value && saved.filename == file.getFileName.toString &&
                saved.sourceUrl == sourceUrl,
            "Metadata does not describe the requested source. Use --refresh"
        )
        require(
            saved.bytes == Files.size(file) && saved.sha256 == Digest.sha256(file),
            "Local size or SHA-256 mismatch. Inspect files and use --refresh"
        )
        Some(saved)
    }
}
