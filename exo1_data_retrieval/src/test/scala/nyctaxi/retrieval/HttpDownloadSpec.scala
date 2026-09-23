package nyctaxi.retrieval

import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.AtomicReference
import nyctaxi.shared.{Digest, Fixtures}
import org.scalatest.funsuite.AnyFunSuite
import scala.util.Using

/** HTTP behavior is tested with deliberately broken local responses and short deadlines. */
class HttpDownloadSpec extends AnyFunSuite {
  private val body = "unchanged bytes\u0000\u0001".getBytes(java.nio.charset.StandardCharsets.UTF_8)
  private val fast = DownloadPolicy(Duration.ofSeconds(1), Duration.ofSeconds(2), 3, 1L)

  test("preserve response bytes, follow redirects, and record the final URL") {
    Fixtures.directory { dir =>
      Fixtures.server { (exchange, _) =>
        if (exchange.getRequestURI.getPath == "/redirect") {
          exchange.getResponseHeaders.add("Location", "/file")
          exchange.sendResponseHeaders(302, -1)
        } else Fixtures.respond(exchange, 200, body)
      } { (base, requests) =>
        Using.resource(new HttpDownload(fast)) { client =>
          val result = client.fetch(base.resolve("redirect"), dir, "file.parquet")
          assert(Files.readAllBytes(result.path).sameElements(body))
          assert(result.bytes == body.length && result.sha256 == Digest.sha256(result.path))
          assert(result.finalUrl == base.resolve("file").toString && requests.get() == 2)
        }
      }
    }
  }

  test("retry 429 and 503 but do not retry 404 or leave error bodies on disk") {
    Fixtures.directory { dir =>
      Fixtures.server { (exchange, number) =>
        Fixtures.respond(exchange, Vector(429, 503, 200)(math.min(number - 1, 2)), body)
      } { (base, requests) =>
        Using.resource(new HttpDownload(fast)) { client =>
          client.fetch(base, dir, "file.parquet")
          assert(requests.get() == 3 && Fixtures.children(dir).size == 1)
        }
      }
    }
    Fixtures.directory { dir =>
      Fixtures.server((exchange, _) => Fixtures.respond(exchange, 404, body)) { (base, requests) =>
        Using.resource(new HttpDownload(fast)) { client =>
          assert(intercept[java.io.IOException](client.fetch(base, dir, "file.parquet")).getMessage == "HTTP 404")
          assert(requests.get() == 1 && Fixtures.children(dir).isEmpty)
        }
      }
    }
  }

  test("exhaust exactly three attempts after persistent server errors") {
    Fixtures.directory { dir =>
      Fixtures.server((exchange, _) => Fixtures.respond(exchange, 500, body)) { (base, requests) =>
        Using.resource(new HttpDownload(fast)) { client =>
          intercept[java.io.IOException](client.fetch(base, dir, "file.parquet"))
          assert(requests.get() == 3 && Fixtures.children(dir).isEmpty)
        }
      }
    }
  }

  test("accept a complete chunked response without Content-Length") {
    Fixtures.directory { dir =>
      Fixtures.server { (exchange, _) =>
        exchange.sendResponseHeaders(200, 0L)
        exchange.getResponseBody.write(body)
      } { (base, _) =>
        Using.resource(new HttpDownload(fast)) { client =>
          assert(Files.readAllBytes(client.fetch(base, dir, "file.parquet").path).sameElements(body))
        }
      }
    }
  }

  test("connection failures remain bounded and leave no candidate") {
    Fixtures.directory { dir =>
      val port = Using.resource(new java.net.ServerSocket(0))(_.getLocalPort)
      Using.resource(new HttpDownload(fast)) { client =>
        intercept[java.io.IOException](client.fetch(java.net.URI.create(s"http://127.0.0.1:$port/"), dir, "file.parquet"))
        assert(Fixtures.children(dir).isEmpty)
      }
    }
  }

  test("reject an empty response and a truncated Content-Length body") {
    for (truncated <- Vector(false, true)) Fixtures.directory { dir =>
      Fixtures.server { (exchange, _) =>
        if (truncated) {
          exchange.sendResponseHeaders(200, body.length + 100L)
          exchange.getResponseBody.write(body)
        } else Fixtures.respond(exchange, 200, Array.emptyByteArray)
      } { (base, _) =>
        Using.resource(new HttpDownload(fast.copy(attemptTimeout = Duration.ofMillis(300), attempts = 1))) { client =>
          val error = intercept[Exception](client.fetch(base, dir, "file.parquet"))
          assert(error.isInstanceOf[java.io.IOException] || error.isInstanceOf[TimeoutException])
          assert(Fixtures.children(dir).isEmpty)
        }
      }
    }
  }

  test("the complete-body deadline cancels stalled transfers and cleans every attempt") {
    Fixtures.directory { dir =>
      Fixtures.server { (exchange, _) =>
        exchange.sendResponseHeaders(200, 100L)
        exchange.getResponseBody.write(1)
        exchange.getResponseBody.flush()
        Thread.sleep(5000)
      } { (base, requests) =>
        Using.resource(new HttpDownload(fast.copy(attemptTimeout = Duration.ofMillis(200), attempts = 2))) { client =>
          val started = System.nanoTime()
          intercept[TimeoutException](client.fetch(base, dir, "file.parquet"))
          assert(Duration.ofNanos(System.nanoTime() - started).toMillis < 2000)
          assert(requests.get() == 2 && Fixtures.children(dir).isEmpty)
        }
      }
    }
  }

  test("thread interruption stops the transfer without retrying") {
    Fixtures.directory { dir =>
      val entered = new CountDownLatch(1)
      Fixtures.server { (exchange, _) =>
        exchange.sendResponseHeaders(200, 100L)
        exchange.getResponseBody.write(1)
        exchange.getResponseBody.flush()
        entered.countDown()
        Thread.sleep(5000)
      } { (base, requests) =>
        Using.resource(new HttpDownload(fast)) { client =>
          val failure = new AtomicReference[Throwable]()
          val worker = new Thread(() => {
            try client.fetch(base, dir, "file.parquet") catch { case error: Throwable => failure.set(error) }
          })
          worker.start()
          assert(entered.await(2, TimeUnit.SECONDS))
          worker.interrupt()
          worker.join(2000)
          assert(!worker.isAlive && failure.get().isInstanceOf[InterruptedException])
          assert(requests.get() == 1 && Fixtures.children(dir).isEmpty)
        }
      }
    }
  }
}
