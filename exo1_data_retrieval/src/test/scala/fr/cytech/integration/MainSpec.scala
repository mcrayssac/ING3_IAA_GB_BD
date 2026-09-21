package fr.cytech.integration

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.funsuite.AnyFunSuite

/** Covers month parsing, source naming, and local staging without touching TLC. */
class MainSpec extends AnyFunSuite {

  private val Month = "2026-05"
  private val Payload = "PAR1-fake-trip-file".getBytes(StandardCharsets.UTF_8)

  /** Serves the monthly file locally so the tests stay offline and fast. */
  private def withServer(status: Int)(body: (String, AtomicInteger) => Unit): Unit = {
    val requests = new AtomicInteger(0)
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/trip-data",
      (exchange: HttpExchange) => {
        requests.incrementAndGet()
        if (status == 200) {
          exchange.sendResponseHeaders(200, Payload.length.toLong)
          exchange.getResponseBody.write(Payload)
        } else {
          exchange.sendResponseHeaders(status, -1L)
        }
        exchange.close()
      }
    )
    server.start()
    try body(s"http://127.0.0.1:${server.getAddress.getPort}/trip-data", requests)
    finally server.stop(0)
  }

  private def withStaging(body: Path => Unit): Unit = {
    val rawDir = Files.createTempDirectory("m2-2-raw")
    try body(rawDir)
    finally {
      Files.walk(rawDir).sorted(java.util.Comparator.reverseOrder()).forEach(Files.delete(_))
    }
  }

  test("month lists are parsed and invalid months are rejected") {
    assert(RawRetrieval.parseMonths(" 2026-05, 2026-06 ,2026-07 ") ==
      Seq("2026-05", "2026-06", "2026-07"))
    assert(RawRetrieval.parseMonths(RawRetrieval.DefaultMonths).size == 3)
    assertThrows[IllegalArgumentException](RawRetrieval.parseMonths(""))
    assertThrows[IllegalArgumentException](RawRetrieval.parseMonths("2026-13"))
    assertThrows[IllegalArgumentException](RawRetrieval.parseMonths("may-2026"))
  }

  test("file names and URLs follow the TLC source catalog") {
    assert(RawRetrieval.fileName(Month) == "yellow_tripdata_2026-05.parquet")
    assert(RawRetrieval.sourceUrl(RawRetrieval.DefaultBaseUrl, Month) ==
      "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-05.parquet")
    assert(RawRetrieval.sourceUrl("http://host/trip-data/", Month) ==
      "http://host/trip-data/yellow_tripdata_2026-05.parquet")
  }

  test("a month is staged unchanged and is not downloaded twice") {
    withStaging { rawDir =>
      withServer(200) { (baseUrl, requests) =>
        val staged = RawRetrieval.run(Seq(Month), rawDir, baseUrl)
        assert(staged == Seq(rawDir.resolve("yellow_tripdata_2026-05.parquet")))
        assert(Files.readAllBytes(staged.head).sameElements(Payload))
        assert(requests.get() == 1)

        RawRetrieval.run(Seq(Month), rawDir, baseUrl)
        assert(requests.get() == 1)
      }
    }
  }

  test("a failed download leaves no file in the staging directory") {
    withStaging { rawDir =>
      withServer(404) { (baseUrl, _) =>
        val failure = intercept[IOException] {
          RawRetrieval.run(Seq(Month), rawDir, baseUrl)
        }
        assert(failure.getMessage.contains("404"))
        assert(!Files.exists(rawDir.resolve("yellow_tripdata_2026-05.parquet")))
      }
    }
  }
}
