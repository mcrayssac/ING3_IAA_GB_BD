package nyctaxi.publication

import java.io.{InputStream, OutputStream}
import java.net.URI
import java.nio.file.AccessDeniedException
import java.time.Duration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.{S3Client, S3Configuration}
import software.amazon.awssdk.services.s3.model.{CreateBucketRequest, HeadBucketRequest, S3Exception}

final case class ObjectInfo(bytes: Long, modifiedMillis: Long)

/** Failed copies must abort, never close a partial stream and accidentally commit it. */
trait ObjectWrite {
  def output: OutputStream
  def commit(): Unit
  def abort(): Unit
}

trait ObjectStore extends AutoCloseable {
  def ensureBucket(): Unit
  def stat(key: String): Option[ObjectInfo]
  def open(key: String): InputStream
  def create(key: String): ObjectWrite
  override def close(): Unit = ()
}

object S3aObjectStore {
  /** The orchestration layer owns retries. Disable nested SDK and S3A retry loops. */
  def settings(config: UploadConfig, policy: StoragePolicy = StoragePolicy()): Map[String, String] = Map(
    "fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "fs.s3a.endpoint" -> config.endpoint.toString,
    "fs.s3a.endpoint.region" -> "us-east-1",
    "fs.s3a.access.key" -> config.accessKey,
    "fs.s3a.secret.key" -> config.secretKey,
    "fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "fs.s3a.path.style.access" -> "true",
    "fs.s3a.connection.ssl.enabled" -> (config.endpoint.getScheme == "https").toString,
    "fs.s3a.create.conditional.enabled" -> "true",
    "fs.s3a.connection.establish.timeout" -> s"${policy.connectTimeout.toMillis}ms",
    "fs.s3a.connection.timeout" -> s"${policy.operationTimeout.toMillis}ms",
    "fs.s3a.connection.request.timeout" -> s"${policy.operationTimeout.toMillis}ms",
    "fs.s3a.attempts.maximum" -> "1",
    "fs.s3a.retry.limit" -> "0",
    "fs.s3a.retry.throttle.limit" -> "0",
    "fs.s3a.input.stream.type" -> "classic",
    "fs.s3a.fast.upload.buffer" -> "disk",
    "fs.s3a.multipart.purge" -> "false"
  )

  def configuration(settings: Map[String, String]): Configuration = {
    val conf = new Configuration()
    settings.foreach { case (key, value) => conf.set(key, value) }
    conf
  }
}

/** Bucket administration uses the existing SDK. Object bytes always travel through S3A. */
final class S3aObjectStore(config: UploadConfig, policy: StoragePolicy = StoragePolicy()) extends ObjectStore {
  private val client = S3Client.builder().endpointOverride(config.endpoint).region(Region.US_EAST_1)
    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(config.accessKey, config.secretKey)))
    .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
    .httpClientBuilder(ApacheHttpClient.builder().connectionTimeout(policy.connectTimeout).socketTimeout(policy.operationTimeout))
    .overrideConfiguration(ClientOverrideConfiguration.builder().apiCallTimeout(policy.operationTimeout)
      .apiCallAttemptTimeout(policy.operationTimeout).retryPolicy(RetryPolicy.builder().numRetries(0).build()).build())
    .build()
  private val fs = FileSystem.newInstance(URI.create(s"s3a://${config.bucket}"),
    S3aObjectStore.configuration(S3aObjectStore.settings(config, policy)))

  private def path(key: String): HadoopPath = new HadoopPath(s"s3a://${config.bucket}/$key")

  override def ensureBucket(): Unit = {
    val head = HeadBucketRequest.builder().bucket(config.bucket).build()
    try client.headBucket(head) catch {
      case error: S3Exception if error.statusCode() == 404 =>
        try client.createBucket(CreateBucketRequest.builder().bucket(config.bucket).build()) catch {
          // A concurrent creator is accepted only after access to the bucket is confirmed.
          case concurrent: S3Exception if concurrent.statusCode() == 409 => client.headBucket(head)
        }
    }
  }

  override def stat(key: String): Option[ObjectInfo] = try {
    val status = fs.getFileStatus(path(key))
    require(status.isFile, s"Object key denotes a directory: $key")
    Some(ObjectInfo(status.getLen, status.getModificationTime))
  } catch { case _: java.io.FileNotFoundException => None }

  override def open(key: String): InputStream = fs.open(path(key))

  override def create(key: String): ObjectWrite = {
    // The builder enables conditional commit, including competing writers that passed HEAD.
    val stream = fs.createFile(path(key)).overwrite(false)
      .opt("fs.s3a.create.performance", true).build()
    new ObjectWrite {
      override def output: OutputStream = stream
      override def commit(): Unit = stream.close()
      override def abort(): Unit = { stream.abort(); () }
    }
  }

  override def close(): Unit = try fs.close() finally client.close()
}
