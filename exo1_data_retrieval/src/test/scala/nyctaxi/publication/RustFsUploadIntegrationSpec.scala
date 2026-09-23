package nyctaxi.publication

import java.nio.file.Files
import java.util.UUID
import nyctaxi.contract.StorageKey
import nyctaxi.shared.Fixtures
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}
import software.amazon.awssdk.services.s3.model._
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Explicit testOnly invocation is required. Every object belongs to a unique disposable bucket. */
@DoNotDiscover
class RustFsUploadIntegrationSpec extends AnyFunSuite {
  private def fixture(action: (UploadConfig, S3aObjectStore, S3Client) => Unit): Unit = Fixtures.directory { root =>
    val bucket = "m23-test-" + UUID.randomUUID().toString
    val config = UploadConfig.parse(Array.empty, sys.env ++ Map("S3_BUCKET" -> bucket, "SPARK_MASTER" -> "local[2]"), root = root)
    val client = S3Client.builder().endpointOverride(config.endpoint).region(Region.US_EAST_1)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(config.accessKey, config.secretKey)))
      .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()).build()
    val store = new S3aObjectStore(config)
    try { store.ensureBucket(); action(config, store, client) } finally {
      store.close()
      try {
        // Never run a broad purge against nyc-taxi or a caller-supplied bucket.
        val uploads = client.listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(bucket).build())
        uploads.uploads().asScala.foreach(upload => client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
          .bucket(bucket).key(upload.key()).uploadId(upload.uploadId()).build()))
        client.listObjectsV2Paginator(ListObjectsV2Request.builder().bucket(bucket).build()).contents().asScala
          .foreach(obj => client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(obj.key()).build()))
        client.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build())
      } finally client.close()
    }
  }

  test("conditional S3A creation protects existing keys and competing single and multipart writes") {
    fixture { (_, store, _) =>
      val publisher = new ObjectPublisher(store, new StorageOperations())
      val payload = Payload.memory(Array[Byte](1, 2, 3))
      assert(publisher.publish(StorageKey("existing"), payload) == "uploaded")
      val original = store.stat(StorageKey("existing"))
      assert(publisher.publish(StorageKey("existing"), payload) == "reused" && store.stat(StorageKey("existing")) == original)
      intercept[PublicationConflict](publisher.publish(StorageKey("existing"), Payload.memory(Array[Byte](3, 2, 1))))
      for (blocks <- Vector(1, 70)) {
        val key = StorageKey(s"race-$blocks")
        // Both streams are opened before either closes, defeating a HEAD-only implementation.
        val first = store.create(key)
        val second = store.create(key)
        val block = Array.fill[Byte](1024 * 1024)(7)
        try {
          for (_ <- 0 until blocks) { first.output.write(block); second.output.write(block.map(b => (b + 1).toByte)) }
          first.commit()
          intercept[Exception](second.commit())
          assert(store.stat(key).get.bytes == blocks.toLong * block.length)
          Using.resource(store.open(key))(stream => assert(stream.read() == 7))
        } finally { first.abort(); second.abort() }
      }
    }
  }

  test("aborting buffered and multipart streams exposes no partial object or pending upload") {
    fixture { (config, store, client) =>
      for (blocks <- Vector(1, 70)) {
        val key = StorageKey(s"abort-$blocks")
        val pending = store.create(key)
        val block = new Array[Byte](1024 * 1024)
        try for (_ <- 0 until blocks) pending.output.write(block) finally pending.abort()
        assert(store.stat(key).isEmpty)
      }
      assert(client.listMultipartUploads(ListMultipartUploadsRequest.builder().bucket(config.bucket).build()).uploads().isEmpty)
    }
  }

  test("remote Parquet decoding verifies all columns and rejects corrupt column bytes with a readable footer") {
    val (bytes, expected) = PublicationFixtures.parquet()
    fixture { (config, store, _) =>
      val publisher = new ObjectPublisher(store, new StorageOperations())
      val verifier = new SparkRemoteVerifier(config)
      try {
        publisher.publish(StorageKey("good.parquet"), Payload.memory(bytes))
        val actual = verifier.verify(StorageKey("good.parquet"))
        assert(actual.rowCount == 10 && actual.sparkSchema == expected.sparkSchema && actual.parquetSchema == expected.parquetSchema)
        Fixtures.directory { root =>
          val file = Files.write(root.resolve("fixture.parquet"), bytes)
          val input = HadoopInputFile.fromPath(new HadoopPath(file.toUri), new org.apache.hadoop.conf.Configuration())
          val chunk = Using.resource(ParquetFileReader.open(input))(_.getFooter.getBlocks.get(0).getColumns.get(1))
          val corrupted = bytes.clone()
          java.util.Arrays.fill(corrupted, chunk.getStartingPos.toInt, (chunk.getStartingPos + chunk.getTotalSize).toInt, 0.toByte)
          publisher.publish(StorageKey("corrupt.parquet"), Payload.memory(corrupted))
          val remote = HadoopInputFile.fromPath(new HadoopPath(s"s3a://${config.bucket}/corrupt.parquet"),
            S3aObjectStore.configuration(S3aObjectStore.settings(config)))
          assert(Using.resource(ParquetFileReader.open(remote))(_.getFooter.getBlocks.get(0).getRowCount) == 10)
          intercept[Exception](verifier.verify(StorageKey("corrupt.parquet")))
        }
      } finally verifier.close()
    }
  }
}
