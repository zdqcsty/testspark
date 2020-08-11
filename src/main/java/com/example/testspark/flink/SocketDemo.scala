package com.example.testspark.flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SocketDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val socket = env.socketTextStream("10.130.7.208",9000)

    socket.print()

    env.execute()

  }

}
