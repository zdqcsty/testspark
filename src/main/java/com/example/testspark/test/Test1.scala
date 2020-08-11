package com.example.testspark.test

import org.apache.spark.sql.SparkSession

object Test1 {

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val session = SparkSession
      .builder().enableHiveSupport().master("local[*]").getOrCreate()

    session.sparkContext.hadoopConfiguration.set("dfs.client.use.datanode.hostname", "true")


    session.sql("insert into ceshi.streaming select * from ceshi.test limit 10")

    //    session.sql("select * from ceshi.test limit 10").show

    session.close()

  }

}
