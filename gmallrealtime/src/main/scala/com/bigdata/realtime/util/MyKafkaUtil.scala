package com.bigdata.realtime.util

import java.lang
import java.util.Properties

import com.sun.scenario.effect.Offset
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtil {

  private val properties: Properties = MyPropertiesUtil.load("config.properties")

  val kafkaBrokerList: String = properties.getProperty("kafka.broker.list")

  var kafkaParam  = collection.mutable.Map(
    "bootstrap.servers" -> kafkaBrokerList,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "realtime_group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false:lang.Boolean)
  )

  def getKafkaStream(topic:String, ssc : StreamingContext) : InputDStream[ConsumerRecord[String,String]] = {
    val inputDs: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
    )
    inputDs
  }

  def getKafkaSream(topic : String, ssc : StreamingContext, offsets: Map[TopicPartition,Long],groupId: String) : InputDStream[ConsumerRecord[String,String]] = {

    kafkaParam("group.id") = groupId

    val kafaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Array(topic), kafkaParam, offsets)
    )
    kafaDStream

  }


}
