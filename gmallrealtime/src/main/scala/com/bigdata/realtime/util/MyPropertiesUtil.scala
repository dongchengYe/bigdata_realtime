package com.bigdata.realtime.util

import java.io.InputStreamReader
import java.util.Properties

object MyPropertiesUtil {

  def load(propertieName:String): Properties = {
    val prop = new Properties()

    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName),"UTF-8"))
    return prop;
  }

  def main(args: Array[String]): Unit = {
    val prop : Properties= MyPropertiesUtil.load("config.properties")

    println(prop.getProperty("redis.host"))

  }
}
