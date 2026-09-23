package nyctaxi.contract

import java.net.URI
import java.nio.file.Path

/** Object key inside the project bucket, relative to the bucket and never a full URI. */
final case class StorageKey(value: String) {
  require(value.nonEmpty && !value.startsWith("/") && !value.contains("://"), s"Invalid storage key: $value")
  override def toString: String = value
}

/** Names and locations shared by retrieval and publication (interfaces I1, I2, I3, and I11).
  *
  * Input: months, filenames, checksums, snapshot identifiers, and publication identifiers.
  * Output: TLC filenames and URLs, local reference paths, object keys, and `s3a://` URIs.
  * Failure: an invalid key throws `IllegalArgumentException` through `StorageKey`.
  */
object StorageLayout {
  /** Original TLC filename, kept unchanged in RAW_DIR and in `nyc_raw/`. */
  def tripFilename(month: Month): String = s"yellow_tripdata_${month.value}.parquet"

  /** Official HTTPS location recorded in the M2.1 source catalog. */
  def tripSource(month: Month): URI =
    URI.create(s"https://d37ci6vzurychx.cloudfront.net/trip-data/${tripFilename(month)}")

  /** Unchanged trip file in the raw zone. */
  def rawTrip(month: Month): StorageKey = StorageKey(s"nyc_raw/${tripFilename(month)}")

  /** Trip sidecar, addressed by its own checksum so each accepted version gets its own key. */
  def tripProvenance(month: Month, sidecarSha256: String): StorageKey =
    StorageKey(s"nyc_metadata/tlc/trips/$sidecarSha256/${tripFilename(month)}.metadata.json")

  /** Reference file of one snapshot, with its source filename. */
  def reference(snapshotId: String, filename: String): StorageKey =
    StorageKey(s"nyc_reference/tlc/$snapshotId/$filename")

  /** Machine-readable descriptor of one reference snapshot. */
  def referenceDescriptor(snapshotId: String): StorageKey =
    StorageKey(s"nyc_metadata/tlc/references/$snapshotId.json")

  /** Immutable receipt of one fully verified publication run. */
  def receipt(publicationId: String): StorageKey =
    StorageKey(s"nyc_metadata/tlc/publications/$publicationId.json")

  /** Local staging path of a reference file below the repository root. */
  def localReference(root: Path, snapshotId: String, filename: String): Path =
    root.resolve(s"data/reference/tlc/$snapshotId/$filename")

  /** S3A URI of the bucket root, used to open the Hadoop file system. */
  def bucketUri(bucket: String): URI = URI.create(s"s3a://$bucket")

  /** S3A URI of one object, used by Hadoop and Spark readers. */
  def uri(bucket: String, key: StorageKey): String = s"s3a://$bucket/${key.value}"
}
