package fr.cytech.integration

import java.nio.file.Paths
import nyctaxi.retrieval.{LocalRetrieval, RetrievalConfig}
import org.scalatest.funsuite.AnyFunSuite

/** The positional command must have the same validation and behavior as runMain. */
class MainSpec extends AnyFunSuite {
  private val root = Paths.get(".").toAbsolutePath.normalize()

  test("defaults and environment settings use the shared configuration") {
    for (env <- Vector(Map.empty[String, String], Map("TAXI_MONTHS" -> "2026-06",
      "RAW_DIR" -> "data/custom", "SPARK_MASTER" -> "local[2]"))) {
      assert(Main.parse(Array.empty, env, root) == RetrievalConfig.parse(Array.empty, env, root))
    }
  }

  test("positional values override only supplied environment settings") {
    val env = Map("TAXI_MONTHS" -> "2026-07", "RAW_DIR" -> "data/env", "SPARK_MASTER" -> "local[2]")
    val subset = Main.parse(Array("2026-05"), env, root)
    assert(subset.months == Vector("2026-05") && subset.rawDir == root.resolve("data/env"))
    val overrideBoth = Main.parse(Array("2026-06,2026-05", "data/path with spaces"), env, root)
    assert(overrideBoth.months == Vector("2026-06", "2026-05"))
    assert(overrideBoth.rawDir == root.resolve("data/path with spaces") && overrideBoth.master == "local[2]")
    val absolute = root.resolve("data/absolute")
    assert(Main.parse(Array("2026-05", absolute.toString), env, root).rawDir == absolute)
  }

  test("refresh accepts zero, one, or two positional arguments") {
    for (positional <- Vector(Array.empty[String], Array("2026-05"), Array("2026-05", "data/raw"))) {
      assert(Main.parse(positional :+ "--refresh", Map.empty, root).refresh)
      assert(!Main.parse(positional, Map.empty, root).refresh)
    }
  }

  test("invalid arguments and configuration fail before execution") {
    for (args <- Vector(Array("--unknown"), Array("--refresh", "2026-05"),
      Array("--refresh", "--refresh"), Array("--help", "2026-05"),
      Array("2026-05", "data/raw", "extra"), Array("2026-05", ""),
      Array("2026-05,2026-05"), Array("2026-08"), Array("2026-5"), Array("2026-05,"))) {
      intercept[IllegalArgumentException](Main.parse(args, Map.empty, root))
    }
    intercept[IllegalArgumentException](Main.parse(Array.empty, Map("SPARK_MASTER" -> "spark://master:7077"), root))
    assert(LocalRetrieval.execute(Main.parse(Array("--unknown"), Map.empty, root)) == 2)
    assert(LocalRetrieval.execute(RetrievalConfig.parse(Array("--unknown"), Map.empty, root)) == 2)
  }
}
