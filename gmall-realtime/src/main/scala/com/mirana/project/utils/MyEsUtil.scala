package com.mirana.project.utils

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}
import scala.collection.JavaConverters._
object MyEsUtil {

  //ES的主机和端口号，工厂类
  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _

  /**
   * 获取客户端
   *
   * @return jestclient
   */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
   * 关闭客户端
   */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }
  /**
   * 建立连接
   */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
      .multiThreaded(true) //允许多线程
      .maxTotalConnection(200) //连接总数
      .connTimeout(10000) //连接超时时间
      .readTimeout(10000)//读取超时时间
      .build)
  }
  // 批量插入数据到ES  (索引名，doc集合[doc_id,source的内容])  source的内容可以是JSON字符串，也可以是对象
  def insertBulk(indexName: String, docList: List[(String, Any)]): Unit = {

    if (docList.nonEmpty) {

      //获取ES客户端连接
      val jest: JestClient = getClient

      //在循环之前创建Bulk.Builder
      val bulkBuilder: Bulk.Builder = new Bulk.Builder()
        .defaultIndex(indexName)
        .defaultType("_doc")

      //循环创建Index对象,并设置进Bulk.Builder
      for ((id, doc) <- docList) {
        //TODO 打印的第一个
        println(id)
        val indexBuilder = new Index.Builder(doc)
        if (id != null) {
          indexBuilder.id(id)
        }
        val index: Index = indexBuilder.build()
        bulkBuilder.addAction(index)
      }

      //创建Bulk对象
      val bulk: Bulk = bulkBuilder.build()

      var items: util.List[BulkResult#BulkResultItem] = null
      try {
        //执行批量写入操作
        //TODO 打印的第二个 这里查看为null
        //println(items) //null
        //jest.execute(bulk)
        items = jest.execute(bulk).getItems
       // println(items)
      } catch {
        case ex: Exception => println(ex.toString)
      } finally {
        close(jest)
        //TODO 尝试注释
        println("保存" + items.size() + "条数据")
        for (item <- items.asScala) {
          if (item.error != null && item.error.nonEmpty) {
            println(item.error)
            println(item.errorReason)
          }
        }
      }
    }
  }
}
