ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.helix"
ThisBuild / version      := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "helix-ingest-v3",

    // Compile against Spark API; cluster provides jars.
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided" // OK for API; CDP will inject its own at runtime
      // Do NOT hard-pin Iceberg/Delta here for CDP runtime. We'll load via spark-submit --conf / cluster libs.
    ),

    // Where your Main lives
    Compile / run / mainClass := Some("com.helix.ingest.Main"),
    Compile / run / fork := true,

    // JVM hygiene (Metals may spellcheck "Duser"—ignore the squiggle)
    Compile / run / javaOptions ++= Seq(
      "-Xms1g",
      "-Xmx2g",
      "-Duser.timezone=UTC",
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),

    // Assembly settings
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  )

