package com.example.testspark.flink

import java.time.Duration
import java.util.{Properties, UUID}

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
//这行代码很关键，做隐士转换所用的
import org.apache.flink.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object Watermark {

  case class People(cid: String, cname: String, ename: String, phone: String, email: String, date: Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //这一行的含义明天研究下
    env.setParallelism(1)

    val stream = env.socketTextStream("10.130.7.208",9000)
      .map { x =>
        val arr = x.split(",")
        val cid = arr(0)
        val cname = arr(1)
        val ename = arr(2)
        val phone = arr(3)
        val email = arr(4)
        val date = arr(5).toLong
        People(cid, cname, ename, phone, email, date)
      }
      .assignAscendingTimestamps(_.date)

    val value = stream
//      .map(x=>(x,1))
      .keyBy(_.cid)
      .timeWindow(Time.seconds(10), Time.seconds(5))
//      .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
//      .window(TumblingEventTimeWindows.of(Time.seconds(50)))
      .reduce(new MyReduceFunction(),new MyWindowFunction)
      .print()

    env.execute()


  }

  class MyReduceFunction extends ReduceFunction[People]{
    override def reduce(value1: People, value2: People): People ={
      if(value1.date>value2.date) value1 else value2
    }
  }

  class MyWindowFunction extends WindowFunction[People, String, String , TimeWindow]{
    override def apply(key: String, timeWindow: TimeWindow, input: Iterable[People], out: Collector[String]): Unit ={
      val people = input.iterator.next()
      val buffer=new StringBuffer()
      buffer.append("窗口的起始时间是-----"+timeWindow.getStart).append("窗口的结束时间是-----"+timeWindow.getEnd).append("输出窗口中最大的时间是----")
        .append(people.cname+"------"+people.date)

      out.collect(buffer.toString)

    }
  }


}
