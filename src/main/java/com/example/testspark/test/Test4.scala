package com.example.testspark.test

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.SparkConf

object Test4 {

  case class Record(id: Int, name: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "67108864b")
//    conf.set("spark.sql.adaptive.join.enabled", "true")
//    conf.set("spark.sql.shuffle.partitions", "1")

    val session = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()



/*    session.sql("select * from hebing.step_step_23953295763_parquet").repartition(1)
        .write.mode(SaveMode.Overwrite).saveAsTable("ceshi.yangli")*/
    session.sql("select msisdn from hebing.step_step_23953295763_parquet group by msisdn")
        .write.mode(SaveMode.Overwrite).saveAsTable("ceshi.yangli")

    //    session.sql("select * from ceshi.test limit 10").show

    session.close()
  }

}
