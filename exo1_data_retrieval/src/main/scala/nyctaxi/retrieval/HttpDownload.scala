package nyctaxi.retrieval

import java.io.IOException
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.{Duration, Instant}
import java.util.concurrent.{ExecutionException, TimeUnit, TimeoutException}
import nyctaxi.shared.{Digest, Retry}

/** Test-injectable limits apply to each complete transfer, including the response body. */
final case class DownloadPolicy(
    connectTimeout: Duration = Duration.ofSeconds(30),
    attemptTimeout: Duration = Duration.ofMinutes(5),
    attempts: Int = 3,
    backoffMillis: Long = 1000L
)

final case class StagedDownload(
    path: Path,
    finalUrl: String,
    retrievedAt: String,
    bytes: Long,
    sha256: String
)

/** Streams successful responses into disposable files and retries only transient failures. */
object HttpDownload {
    private final class HttpFailure(val status: Int) extends IOException(s"HTTP $status")
}

final class HttpDownload(policy: DownloadPolicy = DownloadPolicy()) extends AutoCloseable {
    import HttpDownload.HttpFailure
    private val client = HttpClient.newBuilder().connectTimeout(policy.connectTimeout)
        .followRedirects(HttpClient.Redirect.NORMAL).build()

    private def retryable(error: Throwable): Boolean = error match {
        case http: HttpFailure => http.status == 429 || http.status >= 500 && http.status <= 599
        case _: IOException | _: TimeoutException => true
        case _ => false
    }

    /** The returned candidate belongs to the caller, which must promote or delete it. */
    def fetch(source: URI, directory: Path, filename: String): StagedDownload =
        Retry.run(policy.attempts, policy.backoffMillis, "Download", maxBackoffMillis = 30000L)(retryable)(
            transfer(source, directory, filename)
        )

    /** One complete transfer into a fresh candidate, deleted unless the transfer is accepted. */
    private def transfer(source: URI, directory: Path, filename: String): StagedDownload = {
        val candidate = Files.createTempFile(directory, filename + ".", ".part")
        var keep = false
        try {
            val request = HttpRequest.newBuilder(source).timeout(policy.attemptTimeout)
                .header("Accept-Encoding", "identity").header("User-Agent", "nyc-taxi-retrieval/0.1")
                .GET().build()
            // Discard error bodies rather than saving an HTML error as a Parquet candidate.
            val handler: HttpResponse.BodyHandler[Path] = info =>
                if (info.statusCode() == 200)
                    HttpResponse.BodySubscribers.ofFile(
                        candidate,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING
                    )
                else HttpResponse.BodySubscribers.replacing(candidate)
            val pending = client.sendAsync(request, handler)
            val response = try {
                // ofFile completes only after the body. This deadline also catches a stalled body.
                pending.get(policy.attemptTimeout.toMillis, TimeUnit.MILLISECONDS)
            } catch {
                case interrupted: InterruptedException => pending.cancel(true); throw interrupted
                case timeout: TimeoutException => pending.cancel(true); throw timeout
                case error: ExecutionException => throw error.getCause
            }
            if (response.statusCode() != 200) throw new HttpFailure(response.statusCode())
            val encoding = response.headers().firstValue("Content-Encoding").orElse("identity")
            if (!encoding.equalsIgnoreCase("identity"))
                throw new IOException(s"Unexpected Content-Encoding: $encoding")
            val size = Files.size(candidate)
            val expected = response.headers().firstValueAsLong("Content-Length")
            if (size == 0 || expected.isPresent && expected.getAsLong != size)
                throw new IOException(s"Incomplete body: received $size bytes, Content-Length=$expected")
            val result = StagedDownload(
                candidate,
                response.uri().toString,
                Instant.now().toString,
                size,
                Digest.sha256(candidate)
            )
            keep = true
            result
        } finally {
            if (!keep) Files.deleteIfExists(candidate)
        }
    }

    override def close(): Unit = client.close()
}
