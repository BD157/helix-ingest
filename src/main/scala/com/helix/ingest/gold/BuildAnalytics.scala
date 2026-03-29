package com.helix.ingest.gold

import com.helix.ingest.config.HelixConfig
import com.helix.ingest.iceberg.IcebergOps
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BuildAnalytics {

  def main(args: Array[String]): Unit = {
    val cfg   = HelixConfig.load()
    val table = cfg.gold.outputPath   // e.g. "local.db.gold_sarscov2"

    val spark = buildSession()

    val silver = spark.read.parquet(cfg.silver.outputPath)

    val agg = silver
      .groupBy(col("id"))
      .agg(
        count(lit(1)).as("n_records"),
        sum(col("value")).as("sum_value")
      )
      .orderBy(col("id"))

    // Ensure the Iceberg table exists before writing.
    // CREATE TABLE IF NOT EXISTS is idempotent; schema is inferred from the
    // DataFrame so re-running after a schema-evolution call is safe.
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $table (
        id        STRING,
        n_records LONG,
        sum_value LONG
      )
      USING iceberg
      PARTITIONED BY (collection_year STRING)
    """)

    // Overwrite the whole table on each run — Gold is a recomputed aggregate.
    agg.writeTo(table).overwritePartitions()

    println(s"[Gold] Wrote Iceberg table: $table")

    // Post-write Iceberg operations wired into the pipeline.
    IcebergOps.logSnapshotHistory(spark, table)

    spark.stop()
  }

  /** SparkSession with the local Hadoop Iceberg catalog registered. */
  def buildSession(): SparkSession =
    SparkSession.builder()
      .appName("Helix Gold - BuildAnalytics")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      // ── Iceberg catalog registration ──────────────────────────────────────
      // Enable Iceberg SQL extensions (time travel, CALL procedures, etc.)
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      // Register the "local" catalog backed by the local filesystem.
      // No AWS, no Hive Metastore — just ./warehouse on disk.
      .config("spark.sql.catalog.local",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hadoop")
      .config("spark.sql.catalog.local.warehouse", "./warehouse")
      .getOrCreate()
}
