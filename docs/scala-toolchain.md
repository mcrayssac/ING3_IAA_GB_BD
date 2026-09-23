# Scala Toolchain

Both Scala modules use the same terminal workflow. Each module remains an
independent sbt build. This setup covers the Scala portion of M1.1.

## Shared Versions

| Component | Version | Defined by |
|---|---|---|
| JDK | 21 LTS, maintained 21.x patch | Explicit `JAVA_HOME` |
| sbt | 2.0.9 | Each module's `project/build.properties` |
| Scala | 2.13.18 | Each module's `build.sbt` |
| Spark | 4.2.0 | Each module's `build.sbt` |
| ScalaTest | 3.2.19 | Test dependency in each module's `build.sbt` |

[Spark 4.2.0 supports JDK 21](https://spark.apache.org/docs/4.2.0/)
and [uses Scala 2.13.18](https://github.com/apache/spark/blob/v4.2.0/pom.xml).
[Scala 2.13 supports JDK 21 from 2.13.11](https://docs.scala-lang.org/overviews/jdk-compatibility/overview.html).
[sbt 2 requires JDK 17 or later](https://www.scala-sbt.org/2.x/docs/en/Setup.html)
and can build Scala 2 projects even though its build definitions use Scala 3.

## Select Java and Install sbt

Install a maintained JDK 21 distribution for your operating system and CPU
architecture. A JDK includes both `java` and `javac`. Keep its patch version
updated within the 21.x line.

Replace the placeholder with your JDK installation directory. It must contain
`bin/java` and `bin/javac`.

For Bash or Zsh:

```bash
export JAVA_HOME="/path/to/jdk-21"
export PATH="$JAVA_HOME/bin:$PATH"
java -version
javac -version
```

For PowerShell:

```powershell
$env:JAVA_HOME = 'C:\path\to\jdk-21'
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -version
javac -version
```

Install the [sbt 2.0.9 runner](https://www.scala-sbt.org/2.x/docs/en/Setup.html)
and ensure `sbt` is on `PATH`. No separate Scala or Spark installation is needed.
The runner reads the module's version pin and downloads sbt, the Scala compiler,
and Maven dependencies. The first build requires network access and can take
several minutes.

## Verify the Actual Build Runtime

Run these commands inside each of `exo1_data_retrieval` and
`exo2_data_ingestion`. There is no root sbt build.

```bash
sbt --batch 'about; show scalaVersion; show javaHome; eval System.getProperty("java.version"); shutdown'
```

Expect sbt 2.0.9, project Scala 2.13.18, and Java 21.x. `show javaHome` must point
to your selected JDK. The Scala version used internally by sbt can differ from
the project's compiler version. `sbt --script-version` only identifies the
runner, not the project build version.

sbt 2 keeps a background server. Before changing `JAVA_HOME`, run `sbt shutdown`
in both modules. The next build starts with the new environment. Each build
sets the forked test JVM to the Java installation running that build server.

## Compile and Exercise Spark

Inside each module, run:

```bash
sbt --batch 'clean; update; compile; testFull; shutdown'
sbt --batch 'testFull; shutdown'
```

The first command resolves dependencies, compiles the project and test sources,
and executes all tests, including `ToolchainSpec`. The retrieval module also
tests its [local download workflow](../exo1_data_retrieval/README.md) with small
HTTP and Parquet fixtures. The second command repeats tests from a fresh sbt process.
Use `testFull` for acceptance because sbt 2's `test` can skip successful tests.

The test checks the runtime Java, Scala, and Spark versions, starts Spark with
`local[2]`, counts ten generated rows, and stops Spark in `finally`. Each module
must report a successful toolchain test and print the actual runtime versions.

Tests use a forked JVM with a 1 GB heap and the JDK 21 module access options from
[Spark's launcher](https://github.com/apache/spark/blob/v4.2.0/launcher/src/main/java/org/apache/spark/launcher/JavaModuleOptions.java).
They bind the driver to loopback, disable the Spark web UI, and require no
RustFS, PostgreSQL, Docker cluster, or downloaded dataset.

## Formatting

Scala sources and `build.sbt` use four-space indentation and lines of at most
110 characters. Each module's `.scalafmt.conf` pins scalafmt 3.11.5, and the root
`.editorconfig` applies four-space indentation in editors.

Install the [Coursier](https://get-coursier.io/) launcher, then scalafmt. On macOS:

```bash
brew install coursier
coursier install scalafmt
```

Add the directory printed by the installer to `PATH`. The launcher downloads the
pinned version on first use. Inside each module:

```bash
scalafmt
scalafmt --check
```

The first command formats tracked Scala and sbt files. The second fails when a file
is not formatted.

## Verification Record

Verified on 19 September 2026 using macOS ARM64 and JDK 21.0.12. Both modules
reported sbt 2.0.9, Scala 2.13.18, and Spark 4.2.0. Dependency resolution,
compilation, and one smoke test per module passed. After shutting down both sbt
servers, each module passed its smoke test again in a fresh process.

The check emitted Spark's incubator-vector notice and a native Hadoop fallback
warning. Neither prevented execution. Linux, Windows, other JDK distributions,
and cluster deployment have not been exercised by this verification run.

## Troubleshooting

- **Wrong Java version:** stop the module's sbt server, select JDK 21 through
    `JAVA_HOME`, and rerun the runtime check. Shell `java -version` alone does not
    prove which JVM an existing sbt server uses.
- **Dependency download failure:** check internet access and repository or proxy
    settings. Keep the committed versions while resolving connectivity issues.
- **JVM access errors:** run the documented sbt test command so the forked JVM
    receives the module options. An IDE or manual Java invocation needs equivalent
    options and remains outside this terminal workflow.
- **Local socket restrictions:** Spark needs local driver and executor sockets
    even with `local[2]`. Run the check in an environment allowing loopback sockets.
- **No tests executed:** use `testFull` and confirm the output reports
    `ToolchainSpec` and a successful result. The retrieval module has additional tests.

Generated sbt output and build server metadata are ignored by Git. These checks
verify the local development toolchain. Cluster deployment, storage connections,
application behavior, and deployment packaging belong to later tasks.
