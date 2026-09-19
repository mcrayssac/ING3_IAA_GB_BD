version := "0.1.0-SNAPSHOT"

scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "ex01_data_retrieval",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "4.2.0",
      "org.apache.spark" %% "spark-sql" % "4.2.0",
      "org.apache.spark" %% "spark-hadoop-cloud" % "4.2.0",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    // Keep the test JVM on the JDK selected for sbt through JAVA_HOME.
    javaHome := Some(file(sys.props("java.home"))),
    Test / fork := true,
    Test / parallelExecution := false,
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
