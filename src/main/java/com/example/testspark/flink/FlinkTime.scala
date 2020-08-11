package com.example.testspark.flink

import java.time.Duration
import java.util.Properties

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


object FlinkTime {


  case class Person( value: String)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.130.7.208:9092")
    properties.setProperty("group.id", "demi")

    val kafkaConf = new FlinkKafkaConsumer("test", new SimpleStringSchema(), properties)

    val  stream = env
    .addSource(kafkaConf)


  }


}
