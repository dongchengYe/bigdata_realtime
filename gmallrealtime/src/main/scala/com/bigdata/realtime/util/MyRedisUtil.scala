package com.bigdata.realtime.util

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object MyRedisUtil {

  val jesdisPool:JedisPool = null

  def getJedisClient:Jedis = {

    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val host: String = properties.getProperty("redis.host")
    val port: String = properties.getProperty("redis.port")

    val jedisConfig: JedisPoolConfig = new JedisPoolConfig

    jedisConfig.setMaxTotal(100)
    jedisConfig.setMaxIdle(20)
    jedisConfig.setMinIdle(20)
    jedisConfig.setBlockWhenExhausted(true)
    jedisConfig.setMaxWaitMillis(5000)
    jedisConfig.setTestOnBorrow(true)
    val pool: JedisPool = new JedisPool(jedisConfig,host,port.toInt)

    pool.getResource
  }



}
