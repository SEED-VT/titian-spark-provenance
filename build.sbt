ThisBuild / organization := "edu.vt.bigdebug"
ThisBuild / version := "4.0.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"

val sparkVersion = "4.1.2"

// JDK 17+ module opens required by Spark 4 at runtime
val sparkJavaOptions = Seq(
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
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

lazy val titian = (project in file("."))
  .settings(
    name := "titian",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      // Shipped by Spark itself; compile against the same artifacts.
      "org.roaringbitmap" % "RoaringBitmap" % "1.2.1" % Provided,
      "it.unimi.dsi" % "fastutil" % "8.5.15",
      "com.google.guava" % "guava" % "33.4.0-jre" % Provided,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.apache.spark" %% "spark-core" % sparkVersion % Test,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    javacOptions ++= Seq("--release", "17"),
    // Scaladoc: `sbt doc` -> target/scala-2.13/api/index.html. The root page
    // (src/main/rootdoc.txt) is the API/usage/optimization overview; -groups renders
    // the @group tags on the public API entry points.
    Compile / doc / scalacOptions ++= Seq(
      "-doc-title", "Titian — record-level data provenance for Spark 4",
      "-doc-version", version.value,
      "-doc-root-content", ((Compile / scalaSource).value / "rootdoc.txt").getAbsolutePath,
      "-groups",
      // document internals too (private[spark] capture engine, tap operators, the
      // optimization flags) — the docs are meant to fully explain the code
      "-private",
      "-no-link-warnings"
    ),
    Test / fork := true,
    Test / javaOptions ++= sparkJavaOptions,
    // local-cluster tests: the Worker launches executor JVMs via Spark's launcher,
    // which needs a real distribution (jars/ dir) at SPARK_HOME, plus the Scala
    // version that bin/load-spark-env.sh would normally export.
    Test / envVars ++= Map(
      "SPARK_HOME" ->
        ((ThisBuild / baseDirectory).value / "tools" / "spark-4.1.2-bin-hadoop3").getAbsolutePath,
      "SPARK_SCALA_VERSION" -> "2.13"
    )
  )

lazy val examples = (project in file("examples"))
  .dependsOn(titian)
  .settings(
    name := "titian-examples",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion
    ),
    Compile / run / fork := true,
    Compile / run / javaOptions ++= sparkJavaOptions,
    // run example mains from the repo root so relative paths (tpcds/data, ...) work
    Compile / run / baseDirectory := (ThisBuild / baseDirectory).value
  )
