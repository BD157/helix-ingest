ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "com.helix"
ThisBuild / version      := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "helix-ingest-v3",

    // Compile against Spark API; cluster provides jars.
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-sql"                    % "3.5.1"  % "provided", // CDP will inject at runtime
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.7.1",             // Iceberg Gold sink; bundled in fat-jar
      "com.typesafe"       %  "config"                      % "1.4.3"                // HOCON pipeline config; bundled
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
      // Iceberg registers its catalog/extensions via Java SPI; concatenate so every provider survives.
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case x if x.endsWith("module-info.class") => MergeStrategy.discard
      case _                                    => MergeStrategy.first
    }
  )

