package nyctaxi.publication

import com.fasterxml.jackson.databind.ObjectMapper
import java.net.URI
import java.time.Instant
import nyctaxi.contract.{Month, StorageKey}
import nyctaxi.shared.{RuntimeVersions, Verification}
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

/** The receipt is a published format: field names and order must not drift. */
class ReceiptCodecSpec extends AnyFunSuite {
    private val mapper = new ObjectMapper()

    test("format version 1 keeps its field names and order") {
        val verification = Verification(
            "message schema {}",
            """{"type":"struct","fields":[]}""",
            10L,
            "2026-09-22T00:00:00Z",
            "21",
            "2.13",
            "4.2.0"
        )
        val entry = ReceiptEntry(
            StorageKey("nyc_raw/a"),
            3L,
            "a" * 64,
            "uploaded",
            "2026-09-22T00:00:01Z",
            mapper.readTree("""{"month":"2026-05"}"""),
            ProvenanceEntry(StorageKey("nyc_metadata/p"), 2L, "b" * 64, "reused"),
            Some(verification)
        )
        val receipt = Receipt(
            "nyc-taxi",
            URI.create("http://localhost:9000"),
            "S",
            Vector(Month("2026-05")),
            Vector(entry),
            "R",
            Instant.parse("2026-09-22T00:00:02Z"),
            RuntimeVersions("21", "2.13", "4.2.0")
        )
        val json = mapper.readTree(ReceiptCodec.toJson(receipt))
        assert(json.fieldNames().asScala.toVector == Vector(
            "formatVersion",
            "bucket",
            "endpoint",
            "snapshotId",
            "months",
            "objects",
            "publicationId",
            "completedAt",
            "versions"
        ))
        val item = json.path("objects").get(0)
        assert(item.fieldNames().asScala.toVector ==
            Vector("key", "bytes", "sha256", "action", "verifiedAt", "source", "provenance", "verification"))
        assert(item.path("verification").fieldNames().asScala.toVector ==
            Vector("rowCount", "parquetSchema", "verifiedAt", "sparkSchema", "versions"))
        assert(json.path("months").get(0).asText() == "2026-05" && item.path("key").asText() == "nyc_raw/a")
        assert(item.path("verification").path("sparkSchema").isObject)
    }
}
