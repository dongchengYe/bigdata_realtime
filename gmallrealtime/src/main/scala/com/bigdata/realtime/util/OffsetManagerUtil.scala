package com.bigdata.realtime.util

import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

object OffsetManagerUtil {

  /**
   * 向Redis中保存偏移量
   */
  def saveOffset(topicName:String, groupId:String, offsetRange:Array[OffsetRange]):Unit = {

    if(offsetRange!=null && !offsetRange.isEmpty){
      //创建保存偏移量相关数据的Map
      val offsetMap: util.HashMap[String, String] = new util.HashMap[String,String]()
      for (offset <- offsetRange) {
        offsetMap.put(offset.partition.toString,offset.untilOffset.toString)
        println("保存分区："+offset.partition.toString+"："+offset.fromOffset.toString+"->"+offset.untilOffset.toString)
      }

      //拼接偏移量在Redis中的key
      val offsetKey : String  = "offset:" + topicName + ":" + groupId

      //获取Jedis
      val jedis: Jedis = MyRedisUtil.getJedisClient
      jedis.hmset(offsetKey,offsetMap)
      jedis.close()
    }

  }

  //从Redis中获取偏移量
  def getOffset(topicName:String, groupId:String):Map[TopicPartition,Long] = {

    //获取Redis客户端
    val jedis: Jedis = MyRedisUtil.getJedisClient
    //拼接offset在Redis中的key
    val offsetKey:String = "offset:" + topicName + ":" + groupId

    val topicOffsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)

    import scala.collection.JavaConverters._
    val map: Map[TopicPartition, Long] = topicOffsetMap.asScala.map {
      case (partitionId, untilOffset) => {
        println("读取分区偏移量：" + partitionId + ":" + untilOffset)

        (new TopicPartition(topicName, partitionId.toInt), untilOffset.toLong)
      }
    }.toMap
    map
    //
  }

}
