package com.bigdata.realtime.util

import java.util

import com.bigdata.realtime.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

import scala.collection.mutable.ListBuffer

object MyEsUtil {

  var factory : JestClientFactory = null

  def build() = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(

      new HttpClientConfig.Builder("http://bigdata111:9200")
        .multiThreaded(true)
        .maxTotalConnection(20)
        .connTimeout(10000)
        .readTimeout(1000)
        .build()
    )
  }


  def getClient : JestClient = {
    if(factory == null){
      build()
    }
    factory.getObject
  }


  /**
   * 批量插入数据
   */
  def bulkInsert(sourceList:List[Any], indexName: String):Unit = {

    if(sourceList!=null && !sourceList.isEmpty){
      val jestClient:JestClient = getClient
      val bulkBuilder: Bulk.Builder = new Bulk.Builder

      for (elem <- sourceList) {

        val index: Index = new Index.Builder(elem).index(indexName).`type`("_doc").build()
        bulkBuilder.addAction(index)

      }

      val bulk: Bulk = bulkBuilder.build()
      val result: BulkResult = jestClient.execute(bulk)

      println("插入"+result.getItems.size()+"条数据")
      jestClient.close()
    }


  }


  def main(args: Array[String]): Unit = {
    val daoInfo = DauInfo("zhangsan","lisi","lisi","lisi","lisi","lisi","lisi","ll",1L)

    val infoes = new ListBuffer[DauInfo]()
    infoes+=daoInfo

    bulkInsert(infoes.toList,"index_test")
  }
}
