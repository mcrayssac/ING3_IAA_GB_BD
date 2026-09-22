version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.18"

val retrieve = inputKey[Unit]("Stage the requested months in the local raw directory.")

lazy val root = (project in file("."))
  .settings(
    name := "ex01_data_retrieval",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "4.2.0",
      "org.apache.spark" %% "spark-sql" % "4.2.0",
      "org.apache.spark" %% "spark-hadoop-cloud" % "4.2.0",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.21.2",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    // sbt 2.0.9 forks `run` with unresolved ${OUT} and ${CSR_CACHE} classpath
    // entries, so the retrieval task resolves them through the file converter.
    // It runs from the repository root, where RAW_DIR defaults to data/raw.
    retrieve := {
      val requested = sbt.complete.DefaultParsers.spaceDelimited("[months] [rawDir] [--refresh] | --help").parsed
      val converter = fileConverter.value
      val classpath = (Compile / fullClasspath).value
        .map(entry => converter.toPath(entry.data).toAbsolutePath.toString)
        .mkString(java.io.File.pathSeparator)
      val options = ForkOptions()
        .withJavaHome(javaHome.value)
        .withWorkingDirectory(baseDirectory.value.getParentFile)
        .withRunJVMOptions((Compile / run / javaOptions).value.toVector)
        .withOutputStrategy(Some(LoggedOutput(streams.value.log)))
      val arguments = Seq("-cp", classpath, "fr.cytech.integration.Main") ++ requested
      val exitCode = Fork.java(options, arguments)
      if (exitCode != 0) sys.error(s"Retrieval failed with exit code $exitCode")
    },
    // Keep the test JVM on the JDK selected for sbt through JAVA_HOME.
    javaHome := Some(file(sys.props("java.home"))),
    Test / fork := true,
    Test / parallelExecution := false,
    // Resolve RAW_DIR from the repository root, independent of sbt's module directory.
    Compile / run / fork := true,
    // Wait for the fork before shutdown can remove sbt 2's background-job classpath.
    Compile / runMain := (Compile / fgRunMain).evaluated,
    // Forward child output through sbt's logger so thin clients see per-month results.
    Compile / run / forkOptions := Def.uncached((Compile / run / forkOptions).value
      .withOutputStrategy(Some(LoggedOutput(streams.value.log)))),
    Compile / run / javaOptions := (Test / javaOptions).value.filterNot(_.startsWith("-Xmx")) ++ Seq(
      "-Xmx2g",
      "-Dnyctaxi.repositoryRoot=" + baseDirectory.value.getParentFile.getAbsolutePath
    ),
    // Match Spark 4.2.0 JavaModuleOptions module access when bypassing spark-submit.
    // https://github.com/apache/spark/blob/v4.2.0/launcher/src/main/java/org/apache/spark/launcher/JavaModuleOptions.java
    Test / javaOptions ++= Seq(
      "-Xmx1g",
      "--add-modules=jdk.incubator.vector",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true",
      "--enable-native-access=ALL-UNNAMED"
    )
  )
