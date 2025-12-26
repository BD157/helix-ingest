package com.helix.ingest.silver
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
object StandardizeData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Helix Silver - StandardizeData")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions","1")
      .getOrCreate()
    val bronze = "data/bronze/example"
    val silver = "data/silver/example"
    val df = spark.read.parquet(bronze)
    val standardized = df
      .withColumn("id", col("id").cast(StringType))
      .withColumn("value", col("value").cast(LongType))
      .withColumn("ingest_ts", col("ingest_ts").cast(TimestampType))
      .withColumn("source_system", col("source_system").cast(StringType))
      .withColumn("file_id", col("file_id").cast(StringType))
      .select("id","value","ingest_ts","source_system","file_id")
    standardized.write.mode("overwrite").parquet(silver)
    println(s"Wrote silver to: $silver")
    spark.stop()
  }
}
