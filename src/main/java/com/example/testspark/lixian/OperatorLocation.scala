package com.example.testspark.lixian

import java.util

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object OperatorLocation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("jtw_kafka") .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val frame = spark.read.csv("file:///E:\\data.csv")

    frame.show()

    val hashmap = mutable.HashMap[String,Integer]()

    frame.as[String].foreach {x=>
      if (x.contains("zeng")){
          hashmap.put("zeng",1)
        println("limiande============== "+hashmap.size)
      }
    }

    println("==========================")
    println("外面的   ======================"+hashmap.size)

  }

}
