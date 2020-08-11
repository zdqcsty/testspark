package com.example.testspark.structruedstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object OutputTest {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val spark = SparkSession
      .builder.master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()


    spark.sparkContext.hadoopFile("core-site.xml")
    spark.sparkContext.hadoopFile("hdfs-site.xml")
    spark.sparkContext.hadoopConfiguration.set("HADOOP_USER_NAME","hadoop")

    import spark.implicits._


    val lines = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "10.130.7.208:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "test")
      .load()


    val value = lines.selectExpr("CAST(value AS STRING)").as[String]

    var tuple: (
      String
      ) = null

    val result=value.transform {
      x => {
        x.map {
          x => {
            tuple = ("aaa")
            tuple
          }
        }
      }
    }

//    val query = result.coalesce(1)
//      .writeStream
//      .format("csv")
//      .trigger(Trigger.ProcessingTime("10 seconds"))
//      .outputMode("append")
//      .option("path", "hdfs:///user/zgh/aaa")
////      .option("truncate", "false")
//      .option("checkpointLocation","E:\\check")
//      .start()


        val query = result
          .writeStream
          .format("csv")
          .trigger(Trigger.ProcessingTime("10 seconds"))
          .outputMode("append")
          .option("path", "/user/zgh/aaa")
    //      .option("truncate", "false")
          .option("checkpointLocation","/user/zgh/iolkj")
          .start()

    query.awaitTermination()

  }

}
