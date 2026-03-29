package com.helix.ingest.config

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._

/**
 * Per-layer storage and write settings shared by Bronze, Silver, and Gold.
 *
 * @param outputPath       Destination path or Iceberg table reference
 *                         (e.g. "data/bronze/sarscov2" or "lakehouse.bronze_sarscov2")
 * @param partitionColumns Ordered columns used to partition the output table
 * @param writeMode        Spark save mode: "append" | "overwrite" | "errorIfExists" | "ignore"
 */
case class LayerConfig(
  outputPath:       String,
  partitionColumns: List[String],
  writeMode:        String
)

/**
 * Top-level configuration for the Helix medallion pipeline.
 *
 * Loaded from HOCON (application.conf / application.json / application.properties)
 * via [[HelixConfig.load]].
 *
 * @param bronze    Raw landing zone settings
 * @param silver    Cleansed / deduplicated layer settings
 * @param gold      Aggregated analytics layer settings
 * @param dedupKeys Columns that uniquely identify a record; used by the Silver
 *                  dedup step to pick the latest copy when duplicates arrive
 */
case class HelixConfig(
  bronze:    LayerConfig,
  silver:    LayerConfig,
  gold:      LayerConfig,
  dedupKeys: List[String]
)

object HelixConfig {

  /**
   * Load from the default classpath config (application.conf).
   * System properties (-Dhelix.bronze.output-path=...) and environment
   * variables (HELIX_BRONZE_OUTPUT_PATH) override file values automatically
   * via Typesafe Config's standard substitution chain.
   */
  def load(): HelixConfig = fromTypesafeConfig(ConfigFactory.load())

  /**
   * Load from an explicit file path and fall back to the classpath defaults.
   * Useful when the config is supplied via `spark-submit --files my.conf`
   * and the path is passed as a CLI argument.
   */
  def load(path: String): HelixConfig =
    fromTypesafeConfig(
      ConfigFactory.parseFile(new java.io.File(path))
        .withFallback(ConfigFactory.load())
        .resolve()
    )

  // ── private helpers ────────────────────────────────────────────────────────

  private def fromTypesafeConfig(root: Config): HelixConfig = {
    val cfg = root.getConfig("helix")
    HelixConfig(
      bronze    = parseLayer(cfg.getConfig("bronze")),
      silver    = parseLayer(cfg.getConfig("silver")),
      gold      = parseLayer(cfg.getConfig("gold")),
      dedupKeys = cfg.getStringList("dedup-keys").asScala.toList
    )
  }

  private def parseLayer(c: Config): LayerConfig = LayerConfig(
    outputPath       = c.getString("output-path"),
    partitionColumns = c.getStringList("partition-columns").asScala.toList,
    writeMode        = c.getString("write-mode")
  )
}
