package nyctaxi.publication

import java.nio.file.Files
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID
import nyctaxi.contract.{StorageKey, StorageLayout}
import nyctaxi.shared.{Interrupts, ItemResult, RawDirLock, RuntimeVersions, Verification}
import scala.util.control.NonFatal

/** Per-source results and, for a fully verified run, the key of its receipt. */
final case class PublicationReport(results: Vector[ItemResult], receipt: Option[StorageKey]) {
    def success: Boolean = results.forall(_.success) && receipt.nonEmpty
}

/** One processed source: its result and, once accepted, its receipt entry. */
private final case class PublishedItem(result: ItemResult, entry: Option[ReceiptEntry])

/** Publication of verified local sources to RustFS (interfaces I3 and I11, task M2.3).
  *
  * Input: accepted file/sidecar pairs in RAW_DIR and the staged reference snapshot.
  * Output: objects under nyc_raw/, nyc_reference/, and nyc_metadata/ in the bucket, then one receipt.
  * Failure: items fail independently. The run fails and writes no receipt when any item fails.
  *   Access denial and interruption stop the whole run.
  */
final class PublicationRunner(
    store: ObjectStore,
    verifier: RemoteVerifier,
    inputs: PublicationInputs = new PublicationInputs(),
    policy: StoragePolicy = StoragePolicy(),
    log: String => Unit = println
) {
    private val operations = new StorageOperations(policy)
    private val publisher = new ObjectPublisher(store, operations)
    private val receiptTime = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)

    /** Publishes selected sources, then writes a receipt only if every item verified.
      * Steps: lock RAW_DIR -> ensure bucket -> per item (upload, verify remote, upload provenance)
      *   -> receipt.
      */
    def run(config: UploadConfig): PublicationReport = {
        require(Files.isDirectory(config.rawDir), "RAW_DIR must contain verified local downloads")
        RawDirLock.hold(config.rawDir, "Another retrieval or publication process is using RAW_DIR") {
            operations.run(_ => store.ensureBucket())
            val items = inputs.selected(config).map { case (name, load) => publishItem(config, name, load) }
            // LIMIT: no transaction across objects. A failed run leaves accepted objects for the next rerun.
            val verified = items.forall(_.result.success)
            val receipt = if (verified) Some(writeReceipt(config, items.flatMap(_.entry))) else None
            PublicationReport(items.map(_.result), receipt)
        }
    }

    /** Publishes one source. The single catch point turning an item failure into a redacted result. */
    private def publishItem(
        config: UploadConfig,
        name: String,
        load: () => PublicationInput
    ): PublishedItem = {
        Interrupts.check("Publication")
        val item = try {
            val entry = publishVerified(name, load())
            val decoded = entry.verification.map(v => s", ${v.rowCount} rows fully decoded").getOrElse("")
            val message = s"${entry.action}, remote SHA-256 verified$decoded"
            PublishedItem(ItemResult(name, success = true, message), Some(entry))
        } catch {
            case error if StorageFailures.denied(error) => throw new StorageAccessFailure
            case NonFatal(error) =>
                val message = config.redact(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
                PublishedItem(ItemResult(name, success = false, message), None)
        }
        log(s"$name: ${if (item.result.success) "OK" else "FAILED"} ${item.result.message}")
        item
    }

    /** Uploads the source, checks its remote decoding when it is Parquet, then uploads its provenance. */
    private def publishVerified(name: String, input: PublicationInput): ReceiptEntry = {
        val action = publisher.publish(input.key, input.payload)
        val verification = input.expected.map(expected => verifyRemote(name, input.key, expected))
        val provenanceAction = publisher.publish(input.provenanceKey, input.provenance)
        val provenance = ProvenanceEntry(
            input.provenanceKey,
            input.provenance.bytes,
            input.provenance.sha256,
            provenanceAction
        )
        ReceiptEntry(
            input.key,
            input.payload.bytes,
            input.payload.sha256,
            action,
            Instant.now().toString,
            input.source,
            provenance,
            verification
        )
    }

    /** Fully decodes the remote object and requires the schemas and row count recorded locally. */
    private def verifyRemote(name: String, key: StorageKey, expected: Verification): Verification = {
        val actual = operations.run { scope =>
            scope.onCancel(() => verifier.close())
            val verified = verifier.verify(key)
            scope.check()
            verified
        }
        require(actual.sameAs(expected), s"Remote Parquet verification differs from local provenance: $name")
        actual
    }

    /** Writes the immutable receipt of a fully verified run and returns its key. */
    private def writeReceipt(config: UploadConfig, entries: Vector[ReceiptEntry]): StorageKey = {
        // LIMIT: only a fully verified run gets a receipt. A partial run leaves per-item log lines only.
        val completedAt = Instant.now()
        val id = receiptTime.format(completedAt) + "-" + UUID.randomUUID()
        val receipt = Receipt(
            config.bucket,
            config.endpoint,
            inputs.snapshotId,
            config.months,
            entries,
            id,
            completedAt,
            RuntimeVersions.current
        )
        val key = StorageLayout.receipt(id)
        publisher.publish(key, Payload.memory(ReceiptCodec.toJson(receipt)))
        log(s"Verified publication receipt: ${StorageLayout.uri(config.bucket, key)}")
        key
    }
}
