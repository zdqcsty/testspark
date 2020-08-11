package com.example.testspark.test

import org.apache.spark.sql.SparkSession

object Test3 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder.master("local[*]")
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    //两个topic数据union
    var tuple: (
      String
      ) = null

    //    spark.sparkContext.textFile("E:\\data.txt")


    val frame = spark.read.csv("E:\\data.csv").toDF("aaa", "bbb")
    import spark.implicits._

    val result = frame.transform {
      x => {
        x.map {
          x => {
            tuple = ("aaa")
            tuple
          }
        }
      }
      //        val frame = x.select("aaa")
      //        frame
      //        tuple("aaa")
    }


    result.show()
  }

}
