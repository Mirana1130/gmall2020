package com.mirana.project.app


import com.alibaba.fastjson.JSON
import com.mirana.project.bean.UserInfo
import com.mirana.project.constants.GmallConstant
import com.mirana.project.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @author Mirana
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //TODO 2.读取Kafka User_INFO主题数据创建流
    val userInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.GMALL_USER_INFO,ssc)
    //打印测试
//    userInfoKafkaDStream.foreachRDD(rdd=>{
//      rdd.foreachPartition(iter=>{
//        iter.foreach(record=>{
//          println(record.value())
//        })
//      })
//    })
    //TODO 3.取出Value,

    userInfoKafkaDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //b.遍历写库
        iter.foreach(record=>{
          //待写入数据
          val userJsonStr: String = record.value()
          //转换为样例类对象
          val userInfo: UserInfo = JSON.parseObject(userJsonStr,classOf[UserInfo])
          jedisClient.set(s"UserInfo:${userInfo.id}",userJsonStr)
          //防止冷数据在Redis中长期保存
          jedisClient.expire(s"UserInfo:${userInfo.id}",24*60*60)
        })
        //c.归还连接
        jedisClient.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
