package nyctaxi.publication

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.{IOException, InputStream}
import java.nio.file.{Files, AccessDeniedException}
import java.time.Duration
import java.util.concurrent.{CountDownLatch, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.AtomicInteger
import nyctaxi.retrieval.{Fixtures, RetrievalConfig, Verification}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PublicationSpec extends AnyFunSuite with BeforeAndAfterAll {
  import PublicationFixtures._
  private var fixture: (Array[Byte], Verification) = _
  override def beforeAll(): Unit = { super.beforeAll(); fixture = parquet() }
  private def verifier: RemoteVerifier = new RemoteVerifier {
    override def verify(key: String): Verification = fixture._2
  }
  private def runner(store: ObjectStore, inputs: PublicationInputs, remote: RemoteVerifier = verifier): PublicationRunner =
    new PublicationRunner(store, remote, inputs, policy, _ => ())

  test("configuration shares strict validation, supports positional precedence, and redacts credentials") {
    Fixtures.directory { root =>
      val defaults = UploadConfig.parse(Array.empty, Map.empty, root = root)
      assert(defaults.master == "local[2]" && defaults.months == RetrievalConfig.selectedMonths)
      val chosen = UploadConfig.parse(Array("2026-07", "with spaces"),
        Map("TAXI_MONTHS" -> "invalid", "RAW_DIR" -> ""), positional = true, root = root)
      assert(chosen.months == Vector("2026-07") && chosen.rawDir == root.resolve("with spaces"))
      for (args <- Vector(Array("--refresh"), Array("--help", "2026-05"), Array("2026-05", "x", "y"), Array("2026-05,2026-05")))
        intercept[IllegalArgumentException](UploadConfig.parse(args, Map.empty, positional = true, root = root))
      for (values <- Vector(Map("SPARK_MASTER" -> "spark://master:7077"), Map("S3_ENDPOINT" -> "http://secret@localhost"),
        Map("S3_BUCKET" -> "Bad_Bucket"), Map("S3_ACCESS_KEY" -> ""), Map("S3_SECRET_KEY" -> "")))
        intercept[IllegalArgumentException](UploadConfig.parse(Array.empty, values, root = root))
      intercept[IllegalArgumentException](UploadConfig.parse(Array("2026-05"), Map.empty, root = root))
      assert(!defaults.toString.contains(defaults.secretKey))
      assert(!defaults.redact(defaults.secretKey).contains(defaults.secretKey))
      assert(RustFsUpload.execute(() => throw new IllegalArgumentException("secret")) == 2)
    }
  }

  test("publication verifies sources and metadata, then reruns write only a new receipt") {
    Fixtures.directory { root =>
      val inputs = stage(root, fixture._1, fixture._2)
      val store = new MemoryStore()
      var reads = 0
      val remote = new RemoteVerifier { override def verify(key: String): Verification = { reads += 1; fixture._2 } }
      val first = runner(store, inputs, remote).run(config(root))
      assert(first.success && first.results.size == 3)
      val accepted = store.entries.filterNot(_._1.contains("/publications/")).view.mapValues(_.clone()).toMap
      val writes = store.writes.size
      val second = runner(store, inputs, remote).run(config(root))
      assert(second.success && reads == 2 && first.receipt != second.receipt)
      assert(store.writes.size == writes + 1)
      accepted.foreach { case (key, bytes) => assert(store.entries(key).sameElements(bytes)) }
      val receipt = new ObjectMapper().readTree(store.entries(second.receipt.get))
      assert(receipt.path("objects").size() == 3 && receipt.path("objects").get(0).path("action").asText() == "reused")
      assert(!new String(store.entries(second.receipt.get), "UTF-8").contains(config(root).secretKey))
    }
  }

  test("invalid local provenance fails its month while later months and references continue") {
    for (damage <- Vector("missing", "sidecar", "checksum", "pending", "schema")) Fixtures.directory { root =>
      val inputs = stage(root, fixture._1, fixture._2)
      val file = root.resolve("raw").resolve(RetrievalConfig.filename("2026-05"))
      damage match {
        case "missing" => Files.delete(file)
        case "sidecar" => Files.delete(file.resolveSibling(file.getFileName.toString + ".metadata.json"))
        case "checksum" => Files.write(file, Array[Byte](1, 2, 3))
        case "pending" => Files.writeString(file.resolveSibling(file.getFileName.toString + ".pending"), "interrupted")
        case "schema" => ()
      }
      val store = new MemoryStore()
      val remote = if (damage == "schema") new RemoteVerifier {
        override def verify(key: String): Verification = if (key.contains("2026-05")) fixture._2.copy(rowCount = 99) else fixture._2
      } else verifier
      val report = runner(store, inputs, remote).run(config(root).copy(months = Vector("2026-05", "2026-06")))
      assert(!report.success && report.receipt.isEmpty && report.results.map(_.success) == Vector(false, true, true, true))
      if (damage != "schema") assert(!store.entries.contains("nyc_raw/" + file.getFileName))
    }
  }

  test("a reference checksum conflict prevents its upload and the receipt") {
    Fixtures.directory { root =>
      val inputs = stage(root, fixture._1, fixture._2)
      Files.writeString(root.resolve("data/reference/tlc/20260919T204934Z/taxi_zone_lookup.csv"), "changed")
      val report = runner(new MemoryStore(), inputs).run(config(root))
      assert(report.results.map(_.success) == Vector(true, true, false) && report.receipt.isEmpty)
    }
  }

  test("remote size and same-size checksum conflicts never replace accepted objects") {
    for (content <- Vector(Array[Byte](0), fixture._1.map(b => (b ^ 1).toByte))) Fixtures.directory { root =>
      val inputs = stage(root, fixture._1, fixture._2)
      val store = new MemoryStore()
      val key = "nyc_raw/" + RetrievalConfig.filename("2026-05")
      store.entries(key) = content.clone()
      val report = runner(store, inputs).run(config(root))
      assert(!report.success && store.entries(key).sameElements(content) && !store.writes.contains(key))
    }
  }

  test("missing provenance after partial publication is repaired without replacing data") {
    Fixtures.directory { root =>
      val inputs = stage(root, fixture._1, fixture._2)
      val store = new MemoryStore() {
        var fail = true
        override def create(key: String): ObjectWrite =
          if (fail && key.contains("/trips/")) throw new IOException("temporary outage") else super.create(key)
      }
      val first = runner(store, inputs).run(config(root))
      assert(!first.success && first.receipt.isEmpty && first.results.tail.forall(_.success))
      val key = "nyc_raw/" + RetrievalConfig.filename("2026-05")
      assert(store.writes.count(_ == key) == 1)
      store.fail = false
      assert(runner(store, inputs).run(config(root)).success && store.writes.count(_ == key) == 1)
    }
  }

  test("lost commit responses are reconciled from remote bytes before any second write") {
    val store = new MemoryStore() {
      override def create(key: String): ObjectWrite = {
        val underlying = super.create(key)
        new ObjectWrite {
          override def output = underlying.output
          override def abort(): Unit = underlying.abort()
          override def commit(): Unit = { underlying.commit(); throw new IOException("response lost") }
        }
      }
    }
    assert(new ObjectPublisher(store, new StorageOperations(policy)).publish("object", Payload.memory(Array[Byte](1, 2))) == "reused")
    assert(store.writes.toVector == Vector("object"))
  }

  test("changing input aborts an incomplete stream without committing or retrying") {
    val store = new MemoryStore()
    val payload = Payload.memory(Array[Byte](1, 2)).copy(sha256 = "0" * 64)
    intercept[IllegalArgumentException](new ObjectPublisher(store, new StorageOperations(policy)).publish("object", payload))
    assert(store.writes.isEmpty && store.aborts == 1)
  }

  test("transient operations have three attempts while access failures stop immediately") {
    val operations = new StorageOperations(policy)
    var attempts = 0
    intercept[IOException](operations.run { _ => attempts += 1; throw new IOException("connection lost") })
    assert(attempts == 3)
    attempts = 0
    intercept[StorageAccessFailure](operations.run { _ => attempts += 1; throw new AccessDeniedException("denied") })
    assert(attempts == 1)
  }

  test("stalled response bodies hit a deadline and release resources before retry") {
    val store = new MemoryStore() {
      val active = new AtomicInteger()
      val opened = new AtomicInteger()
      override def open(key: String): InputStream = {
        assert(active.incrementAndGet() == 1)
        opened.incrementAndGet()
        new InputStream {
          private val closed = new java.util.concurrent.atomic.AtomicBoolean(false)
          override def read(): Int = { Thread.sleep(10000); -1 }
          override def close(): Unit = if (closed.compareAndSet(false, true)) active.decrementAndGet()
        }
      }
    }
    store.entries("object") = Array[Byte](1)
    val fast = policy.copy(operationTimeout = Duration.ofMillis(80))
    intercept[TimeoutException](new ObjectPublisher(store, new StorageOperations(fast)).publish("object", Payload.memory(Array[Byte](1))))
    assert(store.opened.get() == 3 && store.active.get() == 0 && store.writes.isEmpty)
  }

  test("interruption stops later items and raw directory locking excludes another writer") {
    Fixtures.directory { root =>
      val inputs = stage(root, fixture._1, fixture._2)
      val store = new MemoryStore()
      val remote = new RemoteVerifier { override def verify(key: String): Verification = throw new InterruptedException("stop") }
      intercept[InterruptedException](runner(store, inputs, remote).run(config(root)))
      assert(!store.entries.keys.exists(_.startsWith("nyc_reference/")))
      val channel = java.nio.channels.FileChannel.open(root.resolve("raw/.retrieval.lock"), java.nio.file.StandardOpenOption.WRITE)
      val lock = channel.lock()
      try intercept[java.nio.channels.OverlappingFileLockException](runner(store, inputs).run(config(root)))
      finally { lock.release(); channel.close() }
    }
  }
}
