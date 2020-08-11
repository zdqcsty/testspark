package com.example.testspark.structruedstreaming

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.DataType

object StreamingDeduplication {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder.master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()


    spark.udf.register("_stringToTs",_stringToTs _)

    import spark.implicits._

    val lines = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","10.130.7.208:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe","test")
      .load()

//    val jsonSchema ="""{"type":"struct","fields":[{"name":"eventTime","type":"string","nullable":true},{"name":"eventType","type":"string","nullable":true},{"name":"userID","type":"string","nullable":true}]}"""

    val value = lines.selectExpr("CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)")

    //.select(from_json(col("value").cast("string"), DataType.fromJson(jsonSchema)).as("value"))
//      .select($"value.*")
//      .withColumn("aaa", functions.callUDF("timezoneToTimestamp", functions.col("eventTime")))
//    val frame = value.withColumn("date",value("value").substr(2,3))
//    val frame = value.withColumn("timestamp", functions.callUDF("_stringToTs",value("value").substr(13,20)))
//    val frame = value.withColumn("timestamp", functions.callUDF("_stringToTs",value("value").substr(13,20)))
//                     .dropDuplicates("value");

//    val demo = frame.withColumn("aaa",frame("value").substr(0,11))

    val result = value.withWatermark("timestamp", "10 seconds").dropDuplicates("value","timestamp")
//    val result = frame.dropDuplicates("value")
//

//    val value = lines.selectExpr("CAST(value AS STRING)")  .as[String].map(
//      x=>x.split(",")(0)
//    )

    val query = result
      .selectExpr("CAST(value AS STRING)","CAST(timestamp AS TIMESTAMP)")
      .writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()


    query.awaitTermination()


  }
  def _stringToTs(s: String): Timestamp = {
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val time = format.parse(s).getTime
    new Timestamp(time)
  }



}

