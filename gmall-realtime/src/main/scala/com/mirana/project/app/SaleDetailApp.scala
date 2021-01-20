package com.mirana.project.app

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.mirana.project.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.mirana.project.constants.GmallConstant
import com.mirana.project.utils.{JDBCUtil, MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * @author Mirana
 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //TODO 2.消费Kafka订单以及订单明细主题数据创建流
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_INFO, ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_ORDER_DETAIL, ssc)
//    //2.1数据流打印测试
//    //print()会在Driver端，而ConsumerRecord不支持序列化，会报序列化错误，这个只在dierect KafkaAPI会出现，receiveAPI不会出现
//    orderInfoKafkaDStream.print()
//    orderDetailKafkaDStream.print()
//    //使用foreachRDD再处理打印
//    orderInfoKafkaDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition(iter=>{
//        iter.foreach(record=>{
//          println(record.value())
//        })
//      })
//    })
//    orderDetailKafkaDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition(iter=>{
//        iter.foreach(record=>{
//          println(record.value())
//        })
//      })
//    })
    //TODO 3.将两个流的数据转化为样例类对象，并转换为元组类型
    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {
      //a.获取value
      val str: String = record.value()
      //b.JSON.parseObject()方法将JSONString类型转换为样例类
      val orderInfo: OrderInfo = JSON.parseObject(str, classOf[OrderInfo])
      //c.数据脱敏
      val consignee_tel: String = orderInfo.consignee_tel
      val tuple: (String, String) = consignee_tel.splitAt(4)
      orderInfo.consignee_tel = tuple._1 + "*******"
      //d.补充时间字段
      orderInfo.create_date = orderInfo.create_time.split(" ")(0)
      orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
      //e.返回结果 指定为元组类型 方便做流的join
      (orderInfo.id, orderInfo)
    })
    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      //注意要的是order_id
      (orderDetail.order_id, orderDetail)
    })
    // 4.做普通的双流join  DStream[(order_id, (某个订单的数据, 某个订单的数据的多条明细)]
    //  如果网络延迟 两个表数据流来的时候可能会join不上 所以这个方法舍弃
    //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
    //    value.print()

    //TODO 4.FullOuterJoin+缓存到redis组合处理
    val fullJoinResult: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)
    //TODO 5.对单挑数据进行操作,考虑用map类算子,因为要和redis连接，所以用mappartition代替map
    val noUserSaleDetailDStream: DStream[SaleDetail] = fullJoinResult.mapPartitions(iter => {
      //获取redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //在这个分区里创建集合存放关联上的订单结果 TODO !为什么用Buffer
      val details: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]

      //这个隐式对象不可序列化，不能放在外面，只能放在分区里
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      //遍历迭代器，对单条数据做处理
      iter.foreach {
        case (orderId, (infoOpt, detailOpt)) =>
          //创建RedisKey
          val infoRedisKey = s"OrderInfo:$orderId"
          val detailRedisKey = s"OrderDetail:$orderId"

          //判断infoOpt是否为空
          if (infoOpt.isDefined) { //isDefined表示有值,Option有两种情况（Some,null）Some表示有值
            //a.infoOpt不为空 提取数据
            val orderInfo: OrderInfo = infoOpt.get
            //a.1 判断detailOpt是否为空 不为空的话表示join上了，数据有关联写入结果
            if (detailOpt.isDefined) {
              val orderDetail: OrderDetail = detailOpt.get
              details += new SaleDetail(orderInfo, orderDetail)
            }
            //a.2 将自身存入redis 在遍历迭代器下创建RedisKey
            //将orderInfo对象转为JSON串  因为orderInfo是样例类对象编译不通过
            val infoStr: String = Serialization.write(orderInfo)
            jedisClient.set(infoRedisKey, infoStr)
            jedisClient.expire(infoRedisKey, 100)
            //a.3查询Detail数据
            val detailSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
            detailSet.asScala.foreach(detailJson => {
              val orderDetail: OrderDetail = JSON.parseObject(detailJson, classOf[OrderDetail])
              details += new SaleDetail(orderInfo, orderDetail)
            })
          } else {
            //b.info数据为空
            //获取Detail数据
            val orderDetail: OrderDetail = detailOpt.get
            if (jedisClient.exists(infoRedisKey)) {
              //b.1
              val infoJson: String = jedisClient.get(infoRedisKey)
              val orderInfo: OrderInfo = JSON.parseObject(infoJson, classOf[OrderInfo])
              details += new SaleDetail(orderInfo, orderDetail)
            } else {
              //b.2
              val detailStr: String = Serialization.write(orderDetail)
              jedisClient.sadd(detailRedisKey, detailStr)
              jedisClient.expire(detailRedisKey, 100)
            }
          }
      }
      jedisClient.close()
      details.toIterator
    })
    //TODO 6.根据UserID查询Redis中的数据,补全用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetailDStream.mapPartitions(iter => {
      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //
      val result: Iterator[SaleDetail] = iter.map(saleDetail => {
        val userRedisKey: String = s"UserInfo:${saleDetail.user_id}"
        //查看Redis中是否存在该用户信息
        if (jedisClient.exists(userRedisKey)) {
          //Redis中存在，查询Redis,说明该用户有下单，即近期活跃过，重置过期时间
          val userInfoStr: String = jedisClient.get(userRedisKey)
          val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo)

          //重置过期时间
          jedisClient.expire(userRedisKey, 24 * 60 * 60)
          saleDetail
        } else {
          //查新Mysql中是否有数据
          //获取连接
          val connection: Connection = JDBCUtil.getConnection
          //查询数据
          val userInfoStr: String = JDBCUtil.getuserDataFromMysql(connection, "select * from user_info where id =?".stripMargin, Array(saleDetail.user_id))

          println(userInfoStr)
          //将数据写入Redis
          jedisClient.set(s"UserInfo:${saleDetail.user_id}", userInfoStr)
          jedisClient.expire(s"UserInfo:${saleDetail.user_id}", 24 * 60 * 60)
          val userInfo: UserInfo = JSON.parseObject(userInfoStr, classOf[UserInfo])
          saleDetail.mergeUserInfo(userInfo)
          connection.close()
          saleDetail
        }
      })
      //归还连接
      jedisClient.close()
      //返回结果
      result
    })
    //TODO 7.将数据写入ES
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val list: List[(String, SaleDetail)] = iter.map(saleDetail => {
          (s"${saleDetail.order_id}_${saleDetail.order_detail_id}", saleDetail)
        }).toList
        val timestr: String = sdf.format(new Date(System.currentTimeMillis()))
          val indexName: String = s"${GmallConstant.GMALL_SALE_DETAIL_ES_PREFIX}_$timestr"
        //传索引名,List(doc_id,source数据)
        MyEsUtil.insertBulk(indexName,list)
      })
    })
    //noUserSaleDetailDStream.print(100)
    //saleDetailDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
