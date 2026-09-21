package fr.cytech.integration

import java.io.IOException
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.util.Using

/** Local retrieval of the selected TLC yellow-taxi monthly Parquet files (M2.2).
  *
  * Files keep their source names and bytes in the local staging directory of
  * interface I2. Publication to RustFS belongs to M2.3 and M2.4.
  */
object RawRetrieval {

  val DefaultMonths = "2026-05,2026-06,2026-07"
  val DefaultRawDir = "data/raw"
  val DefaultBaseUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data"

  private val MonthPattern = """(\d{4})-(\d{2})""".r

  /** Splits a comma-separated month list and rejects anything but `YYYY-MM`. */
  def parseMonths(months: String): Seq[String] = {
    val parsed = months.split(",").map(_.trim).filter(_.nonEmpty).toSeq
    require(parsed.nonEmpty, "No month requested")
    parsed.foreach {
      case month @ MonthPattern(_, monthOfYear) =>
        require(
          monthOfYear.toInt >= 1 && monthOfYear.toInt <= 12,
          s"Month out of range: $month"
        )
      case other => throw new IllegalArgumentException(s"Expected YYYY-MM, got: $other")
    }
    parsed
  }

  /** Official file name of a month, also used as the local and raw object name. */
  def fileName(month: String): String = s"yellow_tripdata_$month.parquet"

  def sourceUrl(baseUrl: String, month: String): String =
    s"${baseUrl.stripSuffix("/")}/${fileName(month)}"

  /** Downloads a month unless the staging directory already holds it.
    *
    * @return the local file, which is left untouched when it already exists
    */
  def retrieve(client: HttpClient, baseUrl: String, rawDir: Path, month: String): Path = {
    val target = rawDir.resolve(fileName(month))
    if (Files.exists(target)) {
      println(s"$month: keeping ${target.toAbsolutePath} (${Files.size(target)} bytes)")
      target
    } else {
      val url = sourceUrl(baseUrl, month)
      println(s"$month: downloading $url")
      // Download beside the target so an interrupted run leaves no partial Parquet file.
      val partial = rawDir.resolve(s"${fileName(month)}.part")
      val request = HttpRequest.newBuilder(URI.create(url)).GET().build()
      val response = client.send(request, HttpResponse.BodyHandlers.ofInputStream())
      if (response.statusCode() != 200) {
        response.body().close()
        throw new IOException(s"$url returned HTTP ${response.statusCode()}")
      }
      Using.resource(response.body()) { body =>
        Files.copy(body, partial, StandardCopyOption.REPLACE_EXISTING)
      }
      val expected = response.headers().firstValueAsLong("content-length")
      val downloaded = Files.size(partial)
      if (expected.isPresent && expected.getAsLong != downloaded) {
        Files.delete(partial)
        throw new IOException(
          s"$url is incomplete: expected ${expected.getAsLong} bytes, got $downloaded"
        )
      }
      Files.move(partial, target, StandardCopyOption.ATOMIC_MOVE)
      println(s"$month: stored ${target.toAbsolutePath} ($downloaded bytes)")
      target
    }
  }

  /** Retrieves every requested month into the staging directory. */
  def run(months: Seq[String], rawDir: Path, baseUrl: String): Seq[Path] = {
    Files.createDirectories(rawDir)
    val client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build()
    try months.map(month => retrieve(client, baseUrl, rawDir, month))
    finally client.close()
  }
}

/** Entry point taking optional `[months] [rawDir]`, else `TAXI_MONTHS` and `RAW_DIR`. */
object Main extends App {
  private val months = args.lift(0).orElse(sys.env.get("TAXI_MONTHS"))
    .getOrElse(RawRetrieval.DefaultMonths)
  private val rawDir = args.lift(1).orElse(sys.env.get("RAW_DIR"))
    .getOrElse(RawRetrieval.DefaultRawDir)

  RawRetrieval.run(
    RawRetrieval.parseMonths(months),
    Paths.get(rawDir),
    RawRetrieval.DefaultBaseUrl
  )
}
