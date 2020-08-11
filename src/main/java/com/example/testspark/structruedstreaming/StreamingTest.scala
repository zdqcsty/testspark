package com.example.testspark.structruedstreaming

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._

object StreamingTest {

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
    val jsonSchema ="""{"type":"struct","fields":[{"name":"eventTime","type":"string","nullable":true},{"name":"eventType","type":"string","nullable":true},{"name":"userID","type":"string","nullable":true}]}"""

    val value = lines.selectExpr("CAST(value AS STRING)")

    value.select(from_json(col("value").cast("string"), DataType.fromJson(jsonSchema)).as("value"))

    val query = value
//      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","topic")
//      .selectExpr("CAST(value AS STRING)")
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()


    query.awaitTermination()

  }
}
