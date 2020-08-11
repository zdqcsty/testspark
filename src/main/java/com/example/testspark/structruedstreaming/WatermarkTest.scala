package com.example.testspark.structruedstreaming

import org.apache.spark.sql.SparkSession

object WatermarkTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder.master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","10.130.7.208:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe","test")
      .load()

//    val value = lines.selectExpr("CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)").as[(String,String)]
    val value = lines.selectExpr("CAST(value AS STRING)").as[String]

    import org.apache.spark.sql.functions._
    val windowedCounts = value
      .withWatermark("timestamp", "1 minutes")
      .groupBy(
      window($"timestamp", "2 minute", "1 minute"),
      $"value"
    ).count()

    val query = windowedCounts
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()


    query.awaitTermination()



  }

}
