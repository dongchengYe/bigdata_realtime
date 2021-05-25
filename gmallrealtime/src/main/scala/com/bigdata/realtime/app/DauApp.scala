package com.bigdata.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.realtime.bean.DauInfo
import com.bigdata.realtime.util.{MyEsUtil, MyKafkaUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

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

    //利用Redis进行去重
    val filteredDStream: DStream[JSONObject] = jsonRecordDstream.mapPartitions {

      jsonRecordItr =>
        //获取Redis客户端
        val jedis = MyRedisUtil.getJedisClient

        println(jedis.ping())
        //定义当前分区过滤后的数据用于返回
        val listBuffer = new ListBuffer[JSONObject]
        //遍历分区中的数据进行过滤
        for (jsonRecord <- jsonRecordItr) {
          val dt: String = jsonRecord.getString("dt")
          val mid: String = jsonRecord.getJSONObject("common").getString("mid")
          val dauKey: String = "dau:" + dt
          val isNew: lang.Long = jedis.sadd(dauKey, mid)
          jedis.expire(dauKey, 24 * 3600)
          if (isNew == 1L) {
            listBuffer.append(jsonRecord)
          }
        }
        jedis.close()
        listBuffer.toIterator
    }

    filteredDStream.count().print()

//    保存到ES
    filteredDStream.foreachRDD{
          jsonIter => {
            val dauList: List[DauInfo] = jsonIter.map {
                jsonObj => {
                  val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                  DauInfo(
                    commonJsonObj.getString("mid"),
                    commonJsonObj.getString("uid"),
                    commonJsonObj.getString("ar"),
                    commonJsonObj.getString("ch"),
                    commonJsonObj.getString("vc"),
                    jsonObj.getString("dt"),
                    jsonObj.getString("hr"),
                    "00",
                    jsonObj.getLong("ts")
                  )
                }
               }.toLocalIterator.toList


            val date: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyEsUtil.bulkInsert(dauList,"gmall_dau_info_"+date)

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
