package nyctaxi.retrieval

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.net.{InetSocketAddress, URI}
import java.nio.file.{Files, Path}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._
import scala.util.Using

/** Small local fixtures keep automated acceptance independent of TLC and external services. */
object Fixtures {
  def directory[A](action: Path => A): A = {
    val path = Files.createTempDirectory("tlc-retrieval-test-")
    try action(path) finally {
      Using.resource(Files.walk(path))(_.iterator().asScala.toVector.reverse.foreach(Files.delete))
    }
  }

  def server[A](handler: (HttpExchange, Int) => Unit)(action: (URI, AtomicInteger) => A): A = {
    val service = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    val executor = Executors.newCachedThreadPool()
    val requests = new AtomicInteger()
    service.setExecutor(executor)
    service.createContext("/", exchange => {
      try handler(exchange, requests.incrementAndGet())
      catch { case _: java.io.IOException => (); case _: InterruptedException => Thread.currentThread().interrupt() }
      finally exchange.close()
    })
    service.start()
    try action(URI.create(s"http://127.0.0.1:${service.getAddress.getPort}/"), requests)
    finally { service.stop(0); executor.shutdownNow() }
  }

  def respond(exchange: HttpExchange, status: Int, bytes: Array[Byte]): Unit = {
    exchange.sendResponseHeaders(status, if (bytes.isEmpty) -1L else bytes.length.toLong)
    if (bytes.nonEmpty) exchange.getResponseBody.write(bytes)
  }

  def children(path: Path): Vector[Path] = Using.resource(Files.list(path))(_.iterator().asScala.toVector)
}
