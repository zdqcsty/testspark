package com.example.testspark.structruedstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object WindowTest {

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


    val value = lines.selectExpr("CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)").as[(String,String)]



//
//
////    val words = value.flatMap(_.split(" ")).toDF("timestamp","word")
//
//    import org.apache.spark.sql.functions._
//    val windowedCounts = value.groupBy(
//      window($"timestamp", "2 minute", "1 minute"),
//      $"value"
//    ).count()

//    val wordCounts = words.groupBy("value").count()

    val query = value
      .writeStream
      .outputMode("update")
      .format("console")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate", "false")
      .start()


    query.awaitTermination()



  }
}
