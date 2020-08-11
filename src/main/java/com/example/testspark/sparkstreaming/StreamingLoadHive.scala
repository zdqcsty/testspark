package com.example.testspark.sparkstreaming

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object StreamingLoadHive {

  def main(args: Array[String]): Unit = {

//    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val spark = SparkSession
      .builder()
      .appName("load_hive")//.master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration.set("dfs.client.use.datanode.hostname", "true")

    val conf = spark.sparkContext

    val ssc = new StreamingContext(conf, Seconds(120))
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

    val result = stream.map(record => (record.value))

    var tuple: (String, String, String, String, String, String) = null

    val aaa = result.map(x => {
      val tup = x.split(",")
      val cid = tup(0)
      val cname = tup(1)
      val ename = tup(2)
      val phoneNum = tup(3)
      val email = tup(4)
      val address = tup(5)
      tuple = (cid, cname, ename, phoneNum, email, address)
      tuple
    }).foreachRDD { rdd =>
      var date: String = new SimpleDateFormat("yyyyMMddHH").format(new Date)
      var fen: String = new SimpleDateFormat("mm").format(new Date).substring(0, 2)
      var p = date + fen

      rdd.toDF("ccard", "cname", "ename", "phone", "email", "address").createOrReplaceTempView("moniaaa")
      val frame = spark.sql("select * from moniaaa limit 10").show()
      //      val frame = spark.sql("select ccard,cname,count(*) from moniaaa group by cid,cname").show()
      //      val frame = spark.sql("insert into ceshi.fenqu partition (p=$p) select ccard,cname,ename,phone,email,address from moniaaa")
      //      frame.write.csv("file:///E:\\bbb" + new Date().getTime.toString)
      //      frame.write.csv("/user/zgh/ooo/aaa"+new Date().toString)

      var result = spark.sql(
        s"""
           |insert into ceshi.fenqu partition (p=$p)
           |select ccard,cname,ename,phone,email,address from moniaaa
        """.stripMargin)

    }

    ssc.start()
    ssc.awaitTermination()


  }
}
