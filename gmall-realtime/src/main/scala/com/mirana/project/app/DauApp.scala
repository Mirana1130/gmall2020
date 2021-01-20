package com.mirana.project.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.mirana.project.bean.StartUpLog
import com.mirana.project.constants.GmallConstant
//import com.mirana.project.constants.GmallConstant
import com.mirana.project.utils.{DauHandler, MyKafkaUtil}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(5))
      //另外一种创建方式  会自动删除一些数据 一般不用StreamingContext.getActiveOrCreate()
    //3.消费Kafka启动主题数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_STARTUP, ssc)
    //4.将每一行数据转换为样例类对象,并补充时间字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      //a.取出数据
      val value: String = record.value()
      //b.转换成样例类
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      //c.将时间戳转换为字符串
      val ts: Long = startUpLog.ts
      val dateStr: String = sdf.format(new Date(ts))

      //d.给时间字段重新赋值
      val dateArr: Array[String] = dateStr.split(" ")
      startUpLog.logDate = dateArr(0)
      startUpLog.logHour = dateArr(1)
      //e.返回数据
      startUpLog
    })
//TODO 缓存数据 过滤数据之前打印
    startUpLogDStream.cache()
    startUpLogDStream.count().print()

    //5.利用Redis做跨批次去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)
//TODO 缓存数据 过滤数据之后打印
    filterByRedisDStream.cache()
    filterByRedisDStream.count().print()

    //6.使用Mid作为Key做同批次去重
    val filterByMid: DStream[StartUpLog] = DauHandler.filterByMid(filterByRedisDStream:DStream[StartUpLog])
//TODO 缓存数据 过滤数据之后打印
    filterByMid.cache()
    filterByMid.count().print()
    //7.将两次去重之后的结果中的Mid写入Redis,给当天后置批次中去重使用
    DauHandler.saveMidToRedis(filterByMid)
    //8.将数据写入Hbase
    filterByMid.foreachRDD(rdd=>{
      rdd.saveToPhoenix("GMALL200720_DAU",
        //Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    ssc.start()
    ssc.awaitTermination()
  }
}