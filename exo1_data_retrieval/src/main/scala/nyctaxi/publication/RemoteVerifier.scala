package nyctaxi.publication

import nyctaxi.contract.{StorageKey, StorageLayout}
import nyctaxi.shared.{SparkParquetVerifier, Verification}
import org.apache.hadoop.fs.{Path => HadoopPath}

/** Full Spark decoding of a published Parquet object (interface I3, task M2.3).
  *
  * Input: the key of an object already accepted by SHA-256 in the configured bucket.
  * Output: the observed schemas and row count, compared with local provenance by the caller.
  * Failure: unreadable or corrupt remote data throws. `close` stops the Spark session and may cancel a read.
  */
trait RemoteVerifier extends AutoCloseable {
  def verify(key: StorageKey): Verification
  override def close(): Unit = ()
}

final class SparkRemoteVerifier(config: UploadConfig, policy: StoragePolicy = StoragePolicy())
  extends RemoteVerifier {
  private val verifier = new SparkParquetVerifier(config.master, S3aObjectStore.settings(config, policy))

  override def verify(key: StorageKey): Verification =
    verifier.verify(new HadoopPath(StorageLayout.uri(config.bucket, key)))

  override def close(): Unit = verifier.close()
}
