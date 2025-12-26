package com.helix.ingest.gold
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object BuildAnalytics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Helix Gold - BuildAnalytics")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions","1")
      .getOrCreate()
    val silver = "data/silver/example"
    val gold   = "data/gold/example_agg"
    val df = spark.read.parquet(silver)
    val agg = df.groupBy(col("id"))
      .agg(count(lit(1)).as("n_records"), sum(col("value")).as("sum_value"))
      .orderBy(col("id"))
    agg.write.mode("overwrite").parquet(gold)
    println(s"Wrote gold to: $gold")
    spark.stop()
  }
}
