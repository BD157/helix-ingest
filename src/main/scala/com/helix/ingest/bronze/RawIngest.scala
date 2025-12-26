package com.helix.ingest.bronze
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object RawIngest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Helix Bronze - RawIngest")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions","1")
      .getOrCreate()
    val input  = "data/raw/example.csv"
    val bronze = "data/bronze/example"
    val df = spark.read.option("header","true").option("inferSchema","true").csv(input)
    val withMeta = df.withColumn("ingest_ts", current_timestamp())
                     .withColumn("source_system", lit("example_csv"))
                     .withColumn("file_id", input_file_name())
    withMeta.write.mode("overwrite").parquet(bronze)
    println(s"Wrote bronze to: $bronze")
    spark.stop()
  }
}
