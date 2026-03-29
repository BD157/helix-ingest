package com.helix.ingest.iceberg

import org.apache.spark.sql.SparkSession

/**
 * Iceberg table management operations for the Helix pipeline.
 *
 * All three methods operate through SparkSession SQL so they work with any
 * catalog type (hadoop, Hive, REST) without changing the call sites.
 *
 * Intended use:
 *   - [[logSnapshotHistory]]  called automatically after every Gold write
 *   - [[rollback]]            called on demand (e.g. bad run detected)
 *   - [[addColumn]]           called when the Gold schema needs a new field
 */
object IcebergOps {

  /**
   * Logs the full snapshot history of an Iceberg table.
   *
   * The history metadata table ($table.history) is a virtual table exposed by
   * Iceberg. Each row is one committed snapshot: timestamp, snapshot ID, parent
   * snapshot ID, and whether it is the current snapshot.
   *
   * Called automatically at the end of every Gold run so the pipeline log
   * always shows the full lineage of the table's state.
   *
   * @param spark   Active SparkSession with the Iceberg catalog registered
   * @param table   Fully-qualified table name, e.g. "local.db.gold_sarscov2"
   */
  def logSnapshotHistory(spark: SparkSession, table: String): Unit = {
    println(s"\n[IcebergOps] Snapshot history for $table:")
    spark
      .sql(s"SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor FROM $table.history ORDER BY made_current_at")
      .show(truncate = false)
  }

  /**
   * Rolls the Gold table back to a specific snapshot.
   *
   * Uses the Iceberg CALL procedure [[local.system.rollback_to_snapshot]],
   * which moves the table's current pointer without rewriting data files —
   * the operation is metadata-only and instant.
   *
   * Typical usage: a post-run validation job detects bad data and calls
   * {{{
   *   IcebergOps.rollback(spark, "local.db.gold_sarscov2", previousSnapshotId)
   * }}}
   *
   * @param spark      Active SparkSession
   * @param table      Fully-qualified table name
   * @param snapshotId Snapshot ID to restore (visible in [[logSnapshotHistory]] output)
   */
  def rollback(spark: SparkSession, table: String, snapshotId: Long): Unit = {
    // Derive catalog name from the table reference (first segment before the dot).
    val catalog = table.split("\\.")(0)
    println(s"[IcebergOps] Rolling back $table to snapshot $snapshotId ...")
    spark.sql(
      s"CALL $catalog.system.rollback_to_snapshot(table => '$table', snapshot_id => $snapshotId)"
    )
    println(s"[IcebergOps] Rollback complete.")
  }

  /**
   * Safely adds a nullable column to an Iceberg table.
   *
   * ALTER TABLE ADD COLUMN on an Iceberg table is a metadata-only operation:
   * no data files are rewritten. Existing rows implicitly return NULL for the
   * new column. Passing a non-nullable column would violate that contract, so
   * callers should always add columns as nullable and backfill separately if
   * needed.
   *
   * Example:
   * {{{
   *   IcebergOps.addColumn(spark, "local.db.gold_sarscov2", "qc_flag", "BOOLEAN")
   * }}}
   *
   * @param spark      Active SparkSession
   * @param table      Fully-qualified table name
   * @param columnName Name of the new column (must not already exist)
   * @param sparkType  Spark SQL type string, e.g. "STRING", "DOUBLE", "TIMESTAMP"
   */
  def addColumn(spark: SparkSession, table: String, columnName: String, sparkType: String): Unit = {
    println(s"[IcebergOps] Adding column $columnName ($sparkType) to $table ...")
    spark.sql(s"ALTER TABLE $table ADD COLUMN $columnName $sparkType")
    println(s"[IcebergOps] Schema evolution complete.")
  }
}
