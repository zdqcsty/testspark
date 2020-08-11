package com.example.testspark.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object ConsumeKakfa {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.130.7.208:9092")
    properties.setProperty("group.id", "testaaaa")

    val kafkaConf = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties)

    kafkaConf.setStartFromEarliest()

    val  stream = env
      .addSource(kafkaConf)

    stream.print()

    env.execute()

  }

}
