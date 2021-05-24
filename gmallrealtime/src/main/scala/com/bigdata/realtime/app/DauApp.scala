package com.bigdata.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.bigdata.realtime.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日活统计
 */
object DauApp {


  def main(args: Array[String]): Unit = {
    /**
     * 第一步消费kafka数据
     */

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dao_app")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))

    val groupId = "gmall_realtime"
    val topic = "gmall_start"

    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc,groupId)
    val jsonRecordDstream = recordDstream.map {
      record =>
        val value = record.value()
        val jSONObject = JSON.parseObject(value)
        val ts = jSONObject.getLong("ts")

        val dateHourStr: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateHourArr = dateHourStr.split(" ")
        jSONObject.put("dt", dateHourArr(0))
        jSONObject.put("hr", dateHourArr(1))

        jSONObject
    }
    jsonRecordDstream.print(1000)


    ssc.start()
    ssc.awaitTermination()
  }



}
