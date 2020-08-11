package com.example.testspark.sparkstreaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Demo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("jtw_kafka").master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val conf = spark.sparkContext

    val ssc = new StreamingContext(conf, Seconds(10))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.130.7.208:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
//356695198012044782,苏响,Ivan,18450244384,aflbth1olb@y2hxe.ms3,山东省菏泽市央袁路4847号遏掉小区19单元475室

    val result = stream.map(record => (record.value))

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    var tuple: (String, String, String, String, String, String)=null

    val aaa= result.map(x=>{
      val tup = x.split(",")
      val cid=tup(0)
      val cname=tup(1)
      val ename=tup(2)
      val phoneNum=tup(3)
      val email=tup(4)
      val address=tup(5)
      tuple=(cid,cname,ename,phoneNum,email,address)
      tuple
    }).foreachRDD { rdd =>
      import spark.implicits._
      rdd.toDF("cid", "cname", "ename", "phoneNum", "email", "address").createOrReplaceTempView("moniaaa")
      //      val frame = spark.sql("select cid,cname from moniaaa limit 10").show()
      val frame = spark.sql("select cid,cname,count(*) from moniaaa group by cid,cname").show()
//      frame.write.csv("file:///E:\\bbb" + new Date().getTime.toString)
      //      frame.write.csv("/user/zgh/ooo/aaa"+new Date().toString)
    }


//    result.foreachRDD { rdd =>
//      // Get the singleton instance of SparkSession
//
//      // Convert RDD[String] to DataFrame
//
//
//
//
//      wordsDf.show()
//      // Create a temporary view
////      wordsDataFrame.createOrReplaceTempView("words")
//
//      // Do word count on DataFrame using SQL and print it
////      val wordCountsDataFrame =
////        spark.sql("select word, count(*) as total from words group by word")
////      wordCountsDataFrame.show()
//    }
//

//    result.print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate



  }

}
