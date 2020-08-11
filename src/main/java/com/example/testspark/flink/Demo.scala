package com.example.testspark.flink

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object Demo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironment

    val unit = env.readTextFile("file:///E:\\data.csv")

    unit.print()

    env.execute()


  }


}
