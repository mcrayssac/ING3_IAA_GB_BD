package fr.cytech.integration

import java.nio.file.{Path, Paths}
import nyctaxi.retrieval.{LocalRetrieval, RetrievalConfig}

/** Retains the retrieve task's positional interface with one shared retrieval engine. */
object Main {
    private val help = """Usage: retrieve [months] [rawDir] [--refresh] | --help
    |Positional months and rawDir override TAXI_MONTHS and RAW_DIR.
    |Months: comma-separated subset of 2026-05,2026-06,2026-07 (default: all three).
    |RAW_DIR defaults to data/raw relative to the repository root.
    |SPARK_MASTER must be local (default: local[*]). .env is not loaded.
    |--refresh verifies replacements before replacing existing files and metadata.
    |Normal reuse requires matching metadata, size, and SHA-256, then a full Spark read.
    |""".stripMargin

    /** Translate positional overrides before applying the shared configuration validation. */
    def parse(
        args: Array[String],
        env: Map[String, String],
        root: Path = Paths.get(sys.props.getOrElse("nyctaxi.repositoryRoot", ""))
    ): RetrievalConfig = {
        val refresh = args.lastOption.contains("--refresh")
        val positional = if (refresh) args.dropRight(1) else args
        require(
            positional.length <= 2 && !positional.exists(_.startsWith("--")),
            "Use retrieve [months] [rawDir] [--refresh] or retrieve --help"
        )
        val overrides = Vector("TAXI_MONTHS", "RAW_DIR").zip(positional).toMap
        val flags = if (refresh) Array("--refresh") else Array.empty[String]
        RetrievalConfig.parse(flags, env ++ overrides, root)
    }

    /** Only the command boundary exits the JVM. Help does not validate the environment. */
    def main(args: Array[String]): Unit = {
        if (args.toVector == Vector("--help")) { println(help); return }
        val status = LocalRetrieval.execute(parse(args, sys.env))
        if (status != 0) System.exit(status)
    }
}
