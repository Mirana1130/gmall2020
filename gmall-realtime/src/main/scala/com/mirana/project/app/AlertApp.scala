package com.mirana.project.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.mirana.project.bean.{CouponAlertInfo, EventLog}
import com.mirana.project.constants.GmallConstant
import com.mirana.project.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._
/**
 * @author Mirana
 */
object AlertApp {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //TODO 2.读取Kafka的事件日志数据，创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_EVENT,ssc)

    //TODO 3.转换为样例类,补充时间字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.map(record => {
      //a.将value转换为样例类对象
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
      //b.补充时间字段
      val dateHourStr: String = sdf.format(new Date(eventLog.ts))
      val dateHourArr: Array[String] = dateHourStr.split(" ")
      eventLog.logDate = dateHourArr(0);
      eventLog.logHour = dateHourArr(1);
      //c.返回对象 直接加上mid,方便后面去重
      (eventLog.mid, eventLog)
    })

    //TODO 4.开窗
    val windowDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //TODO 5.按照Mid进行分组
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = windowDStream.groupByKey()

    //TODO 6.在组内对数据进项筛选
    val boolToAlertInfo: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.map { case (mid, iter) => {
      //a.创建HashSet用于存放uid,做到去重
      val uids: util.HashSet[String] = new util.HashSet[String]()
      //b.创建HashSet用于存放Itemid
      val itemIds: util.HashSet[String] = new util.HashSet[String]()
      //c.创建List用于存放行为数据
      val events: util.ArrayList[String] = new util.ArrayList[String]()

      //定义一个标志，用于区分是否有浏览行为
      var noclick: Boolean = true;

      //d.遍历迭代器，向三个集合中添加数据，注意条件

      breakable {
        iter.foreach(log => {
          //提取时间行为
          val evid: String = log.evid
          //将时间行为放入events
          events.add(evid)
          //判断当前行为是否为领券行为
          if ("coupon".equals(evid)) {
            uids.add(log.uid)
            itemIds.add(log.itemid)
          }
          else if ("clickItem".equals(evid)) {
            noclick = false
            break()
          }
        })
      }
      //e.根据uids判断是否需要产生预警日志 第一种，后面需要对null值处理
      //      if(uids.size()>=3 && noclick){
      //        //产生预警日志
      //        CouponAlertInfo(mid,uids,itemIds,events,System.currentTimeMillis())
      //      }else{
      //        null
      //      }
      //e.根据uids判断是否需要产生预警日志
      (uids.size() >= 3 && noclick, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
    }
    }
    val alertInfo: DStream[CouponAlertInfo] = boolToAlertInfo.filter(_._1).map(_._2)

    //TODO 7.给定DocId
    val docIdToAlertInfoDStream: DStream[(String, CouponAlertInfo)] = alertInfo.map(alertInfo => {
      (s"${alertInfo.mid}-${alertInfo.ts / 1000L / 60}", alertInfo)
    })
    //docIdToAlertInfoDStream.print()

    //TODO 8.将数据写入ES
    docIdToAlertInfoDStream.foreachRDD(rdd=>{
      println("计算开始了")
      rdd.foreachPartition(iter=>{
        val date: String = sdf.format(new Date(System.currentTimeMillis())).split(" ")(0)
        MyEsUtil.insertBulk(s"${GmallConstant.GMALL_ALERT_INFO_PREFIX}_${date}",iter.toList)
      })
    })
    //TODO 9.开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
