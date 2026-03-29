# helix-ingest

Deterministic Spark/Scala ingestion framework implementing a Bronze → Silver → Gold medallion pipeline, built to behave like a production data platform.

The Gold layer is backed by **Apache Iceberg** with a local Hadoop catalog — giving the pipeline ACID writes, time travel, and zero-rewrite schema evolution without any external services.

---

## Stack

| Component | Version |
|---|---|
| Scala | 2.12.18 |
| Apache Spark | 3.5.1 |
| Apache Iceberg | 1.7.1 (iceberg-spark-runtime-3.5_2.12) |
| Typesafe Config | 1.4.3 |
| Build tool | sbt 1.10.2 |

---

## Pipeline overview

```
Raw CSV/JSON/TSV
      │
      ▼
┌─────────────┐
│   Bronze    │  RawIngest.scala — schema normalization, metadata stamping,
│  (Parquet)  │  quality gates, SHA-256 row hash. Source-of-truth; append-only.
└─────────────┘
      │
      ▼
┌─────────────┐
│   Silver    │  StandardizeData.scala — type casting, column projection,
│  (Parquet)  │  dedup by (sample_id, collection_date, source_system).
└─────────────┘
      │
      ▼
┌─────────────┐
│    Gold     │  BuildAnalytics.scala — groupBy aggregation written to a
│  (Iceberg)  │  managed Iceberg table: local.db.gold_sarscov2
└─────────────┘
```

---

## Why Iceberg at Gold

| Capability | What it means for this pipeline |
|---|---|
| **ACID writes** | Each Gold run is an atomic commit — readers always see a consistent snapshot, never a partial overwrite |
| **Time travel** | Query any previous Gold snapshot by timestamp or snapshot ID; full audit trail at no extra cost |
| **Schema evolution** | Add nullable columns to the Gold table without rewriting a single data file (`ALTER TABLE ADD COLUMN`) |
| **Snapshot rollback** | Revert the Gold table to any prior snapshot in milliseconds — metadata-only operation |

---

## Configuration

All paths, partition columns, write modes, and dedup keys live in [`src/main/resources/application.conf`](src/main/resources/application.conf) (HOCON). Override any value at runtime with a JVM system property:

```
-Dhelix.gold.output-path=local.db.gold_sarscov2_v2
```

The Gold `output-path` is a fully-qualified Iceberg table name (`catalog.database.table`). The local Hadoop catalog writes metadata and data files under `./warehouse/` relative to the working directory.

---

## Running locally

### Prerequisites

- JDK 11 or 17 (recommended; JDK 8 works with the `--add-exports` flags already in `build.sbt`)
- sbt 1.10.2+

### Run the full pipeline

Each layer has its own `main`. Run them in order:

```bash
# Bronze — reads data/raw/example.csv, writes data/bronze/example
sbt "runMain com.helix.ingest.bronze.RawIngest"

# Silver — reads data/bronze/example, writes data/silver/example
sbt "runMain com.helix.ingest.silver.StandardizeData"

# Gold — reads data/silver/example, writes ./warehouse/db/gold_sarscov2 (Iceberg)
sbt "runMain com.helix.ingest.gold.BuildAnalytics"
```

After the Gold run, snapshot history is printed automatically to the console.

### Iceberg operations

```scala
import com.helix.ingest.iceberg.IcebergOps
import com.helix.ingest.gold.BuildAnalytics

val spark = BuildAnalytics.buildSession()
val table = "local.db.gold_sarscov2"

// Print the full snapshot history
IcebergOps.logSnapshotHistory(spark, table)

// Roll back to a specific snapshot (ID from history output)
IcebergOps.rollback(spark, table, snapshotId = 8765432109876543210L)

// Add a new nullable column — no data rewrite
IcebergOps.addColumn(spark, table, columnName = "qc_flag", sparkType = "BOOLEAN")
```

### Build the fat-jar

```bash
sbt assembly
# → target/scala-2.12/helix-ingest-v3_2.12-0.1.0.jar
```

The Iceberg runtime and Typesafe Config are bundled. Spark is `provided` and must be on the cluster classpath.

---

## Project layout

```
src/main/
├── resources/
│   └── application.conf          # HOCON pipeline config
└── scala/com/helix/ingest/
    ├── Main.scala                 # Production Bronze ingest (SARS-CoV-2 schema)
    ├── bronze/RawIngest.scala     # Local Bronze example
    ├── silver/StandardizeData.scala
    ├── gold/BuildAnalytics.scala  # Gold — Iceberg sink
    ├── config/HelixConfig.scala   # Typesafe Config loader
    └── iceberg/IcebergOps.scala   # Snapshot history, rollback, schema evolution
```
