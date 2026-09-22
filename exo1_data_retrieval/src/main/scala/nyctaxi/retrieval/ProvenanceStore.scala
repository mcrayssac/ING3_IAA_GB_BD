package nyctaxi.retrieval

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import java.io.IOException
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.time.Instant
import java.util.UUID

/** Version 1 records the exact local bytes and the observations made before acceptance. */
final case class Provenance(
  month: String, filename: String, sourceUrl: String, finalUrl: String,
  retrievedAt: String, bytes: Long, sha256: String, verification: Verification
)

/** Owns sidecar validation and detects a crash between the two atomic file replacements. */
final class ProvenanceStore {
  private val mapper = new ObjectMapper()

  def metadata(file: Path): Path = file.resolveSibling(file.getFileName.toString + ".metadata.json")
  def pending(file: Path): Path = file.resolveSibling(file.getFileName.toString + ".pending")

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

  private def read(path: Path): Provenance = {
    val node = mapper.readTree(path.toFile)
    require(node != null && node.isObject && number(node, "formatVersion") == 1L, "Unsupported metadata format")
    val checks = node.path("verification")
    val versions = checks.path("versions")
    val schema = checks.path("sparkSchema")
    require(schema.isObject, "Missing Spark schema")
    val report = Verification(text(checks, "parquetSchema"), schema.toString,
      number(checks, "rowCount"), text(checks, "verifiedAt"), text(versions, "java"),
      text(versions, "scala"), text(versions, "spark"))
    val result = Provenance(text(node, "month"), text(node, "filename"), text(node, "sourceUrl"),
      text(node, "finalUrl"), text(node, "retrievedAt"), number(node, "bytes"), text(node, "sha256"), report)
    require(result.bytes > 0 && report.rowCount >= 0 && result.sha256.matches("[0-9a-f]{64}"),
      "Invalid size, count, or checksum in metadata")
    Instant.parse(result.retrievedAt)
    Instant.parse(report.verifiedAt)
    result
  }

  /** No sidecar is created or repaired implicitly for an existing file. */
  def existing(file: Path, month: String, sourceUrl: String): Option[Provenance] = {
    val sidecar = metadata(file)
    if (Files.exists(pending(file))) throw new IOException("Interrupted promotion detected. Inspect files and use --refresh")
    if (!Files.exists(file) && !Files.exists(sidecar)) return None
    if (!Files.isRegularFile(file) || !Files.isRegularFile(sidecar))
      throw new IOException("Incomplete file/metadata pair. Inspect files and use --refresh")
    val saved = read(sidecar)
    require(saved.month == month && saved.filename == file.getFileName.toString && saved.sourceUrl == sourceUrl,
      "Metadata does not describe the requested source. Use --refresh")
    require(saved.bytes == Files.size(file) && saved.sha256 == FileDigest.sha256(file),
      "Local size or SHA-256 mismatch. Inspect files and use --refresh")
    Some(saved)
  }

  private def json(saved: Provenance): ObjectNode = {
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
    node
  }

  /** Candidates are already verified. A marker makes any incomplete promotion fail closed. */
  def publish(candidate: Path, file: Path, saved: Provenance): Unit = {
    val sidecar = metadata(file)
    val stagedMetadata = Files.createTempFile(file.getParent, file.getFileName.toString, ".metadata.part")
    try {
      mapper.writerWithDefaultPrettyPrinter().writeValue(stagedMetadata.toFile, json(saved))
      if (Files.exists(sidecar)) {
        val history = Files.createDirectories(file.getParent.resolve(".history").resolve(file.getFileName.toString))
        // Portable names avoid ':' on Windows and retain even malformed prior metadata for inspection.
        Files.copy(sidecar, history.resolve(s"${Instant.now().toEpochMilli}-${UUID.randomUUID()}.json"))
      }
      Files.writeString(pending(file), saved.sha256 + "\n", StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      Files.move(candidate, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
      Files.move(stagedMetadata, sidecar, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
      Files.delete(pending(file))
    } finally Files.deleteIfExists(stagedMetadata)
  }
}
