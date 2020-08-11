package com.example.testspark.flink

import java.util.{Properties, UUID}

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkWindow {

  case class People(cid: String, cname: String, ename: String, phone: String, email: String, address: String)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.130.7.208:9092")
    properties.setProperty("group.id", UUID.randomUUID().toString)

    val kafkaConf = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties)

    kafkaConf.setStartFromEarliest()

    val stream = env
      .addSource(kafkaConf)
      .map { x =>
        val arr = x.split(",")
        val cid = arr(0)
        val cname = arr(1)
        val ename = arr(2)
        val phone = arr(3)
        val email = arr(4)
        val address = arr(5)
        People(cid, cname, ename, phone, email, address)
      }

    //滚动窗口示例(按照cid分组统计各个cid的行数)
    val value = stream
      .map(x => (x, 1))
      .keyBy(_._1.cid)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce((x, y) => (x._1, x._2 + y._2))
      .print()

    /*    val value = stream
          .map(x=>(x,1))
          .keyBy(_._1.cid)
          .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
          .reduce((x,y)=>(x._1,x._2+y._2))
          .print()*/


    env.execute()
  }

  class AverageAggregate extends AggregateFunction[String, Int, Int] {
    override def createAccumulator() = 0

    override def add(value: String, accumulator: Int) =
      accumulator + 1

    override def getResult(accumulator: Int) = accumulator

    override def merge(a: Int, b: Int) = a + b
  }

}
