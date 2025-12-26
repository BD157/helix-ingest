package com.helix.ingest

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Main {

  // ----- Canonical SARS-CoV-2 schema -----
  val CanonicalSchema: StructType = StructType(Seq(
    StructField("sample_id", StringType, nullable = false),
    StructField("run_id", StringType, true),
    StructField("collection_date", DateType, true),
    StructField("received_date", DateType, true),
    StructField("reported_date", DateType, true),

    StructField("submitting_lab", StringType, true),
    StructField("geo_region", StringType, true),
    StructField("geo_county", StringType, true),
    StructField("submission_platform", StringType, true),

    StructField("instrument", StringType, true),
    StructField("read_length", IntegerType, true),
    StructField("coverage_mean", DoubleType, true),
    StructField("coverage_median", DoubleType, true),
    StructField("genome_coverage_pct", DoubleType, true),
    StructField("n_content_pct", DoubleType, true),
    StructField("qc_pass", BooleanType, true),
    StructField("qc_notes", StringType, true),

    StructField("ct_orf1ab", DoubleType, true),
    StructField("ct_n", DoubleType, true),
    StructField("ct_s", DoubleType, true),

    StructField("pango_lineage", StringType, true),
    StructField("nextclade_clade", StringType, true),
    StructField("aa_mutations", StringType, true),
    StructField("nt_mutations", StringType, true),
    StructField("ref_genome", StringType, true),
    StructField("assembly_length", IntegerType, true),

    StructField("notes", StringType, true),

    // system metadata
    StructField("ingest_ts", TimestampType, false),
    StructField("source_system", StringType, false),
    StructField("file_id", StringType, false),
    StructField("row_hash", StringType, true)
  ))

  // ----- Entrypoint -----
  def main(args: Array[String]): Unit = {
    val conf = parseArgs(args)
    val spark = SparkSession.builder()
      .appName("Helix-Bronze-Ingest")
      .getOrCreate()

    // Required for Iceberg SQL use
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val raw = readRaw(spark, conf.input, conf.formatHint)
    val shaped = normalize(raw)
      .transform(df => stampMetadata(df, conf.source))
      .transform(df => selectCanonical(df))

    val filtered = qualityGates(shaped)
    writeBronze(filtered, conf)

    spark.stop()
  }

  // ----- CLI args -----
  case class IngestConf(
    input: String,
    basePath: String,
    source: String,
    tableFullName: String,     // e.g., lakehouse.bronze_sarscov2
    formatHint: Option[String] // csv/tsv/json or None
  )
  private def parseArgs(args: Array[String]): IngestConf = {
    // super simple arg parser: --key value
    val m = args.grouped(2).collect { case Array(k, v) if k.startsWith("--") => k.stripPrefix("--") -> v }.toMap
    IngestConf(
      input        = m("input"),
      basePath     = m.getOrElse("base", "/datalake/bronze/sarscov2/v1"),
      source       = m.getOrElse("source", "unknown"),
      tableFullName= m.getOrElse("table", "lakehouse.bronze_sarscov2"),
      formatHint   = m.get("format")
    )
  }

  // ----- IO -----
  private def readRaw(spark: SparkSession, path: String, hint: Option[String]): DataFrame = {
    val (fmt, opts) = hint.map(_.toLowerCase) match {
      case Some("json") => ("json", Map("multiLine" -> "true"))
      case Some("tsv")  => ("csv",  Map("header" -> "true", "sep" -> "\t", "quote" -> "\""))
      case _            => ("csv",  Map("header" -> "true", "sep" -> ",", "quote" -> "\""))
    }
    spark.read.options(opts).format(fmt).load(path)
  }

  // ----- Shaping -----
  private def normalize(df0: DataFrame): DataFrame = {
    val df = df0.columns.foldLeft(df0)((acc, c) =>
      acc.withColumnRenamed(c, c.trim.toLowerCase.replaceAll("\\s+", "_"))
    )

    df
      // Dates: accept common aliases
      .withColumn("collection_date", to_date(coalesce(col("collection_date"), col("sample_collection_date"), col("date_collected"))))
      .withColumn("received_date",   to_date(coalesce(col("received_date"), col("lab_received_date"))))
      .withColumn("reported_date",   to_date(coalesce(col("reported_date"), col("submission_date"))))
      // Numerics
      .withColumn("read_length", col("read_length").cast("int"))
      .withColumn("coverage_mean", col("coverage_mean").cast("double"))
      .withColumn("coverage_median", col("coverage_median").cast("double"))
      .withColumn("genome_coverage_pct", col("genome_coverage_pct").cast("double"))
      .withColumn("n_content_pct", col("n_content_pct").cast("double"))
      .withColumn("qc_pass", col("qc_pass").cast("boolean"))
      .withColumn("ct_orf1ab", col("ct_orf1ab").cast("double"))
      .withColumn("ct_n", col("ct_n").cast("double"))
      .withColumn("ct_s", col("ct_s").cast("double"))
      // Ensure sample_id exists
      .transform { d =>
        if (!d.columns.contains("sample_id"))
          d.withColumn("sample_id", coalesce(col("accession"), col("id")).cast("string"))
        else d
      }
  }

  private def stampMetadata(df: DataFrame, sourceSystem: String): DataFrame = {
    val hashedFile = sha2(coalesce(input_file_name(),
      concat_ws("|", df.columns.map(c => col(c).cast("string")): _*)
    ), 256)

    val hashedRow = sha2(concat_ws("||", df.columns.sorted.map(c => col(c).cast("string")): _*), 256)

    df.withColumn("ingest_ts", current_timestamp())
      .withColumn("source_system", lit(sourceSystem))
      .withColumn("file_id", hashedFile)
      .withColumn("row_hash", hashedRow)
  }

  private def selectCanonical(df: DataFrame): DataFrame = {
    val targetCols = CanonicalSchema.fieldNames.map(n => col(n))
    df.select(targetCols: _*)
      .withColumn("ingest_ts", col("ingest_ts")) // keep not-null contract
  }

  private def qualityGates(df: DataFrame): DataFrame = {
    val minDate = lit("2019-11-01")
    val maxDate = current_date().cast("date")
    df.filter(col("sample_id").isNotNull)
      .withColumn("collection_date",
        when(col("collection_date").between(minDate, maxDate), col("collection_date")).otherwise(lit(null).cast("date")))
  }

  // ----- Bronze write (Iceberg) -----
  private def writeBronze(df: DataFrame, conf: IngestConf): Unit = {
    val out = df.withColumn("ingest_date", to_date(col("ingest_ts")))
    // Table should be pre-created with partition spec on ingest_date
    out.writeTo(conf.tableFullName).append()
  }
}

