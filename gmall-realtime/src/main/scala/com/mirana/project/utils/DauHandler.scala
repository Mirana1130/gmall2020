package com.mirana.project.utils

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.mirana.project.bean.StartUpLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis



object DauHandler {
  /**
   * TODO 利用Mid 做同跨批次去重
   */
  def filterByMid(filterByRedisDStream: DStream[StartUpLog]) = {

    //将数据转换为元组
    val midDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByRedisDStream.map(log => {
      ((log.mid, log.logDate), log)
    })
    //
    val midDataToLogIterDStream: DStream[((String, String), Iterable[StartUpLog])] = midDateToLogDStream.groupByKey()
    //
    val value1: DStream[StartUpLog] = midDataToLogIterDStream.flatMap {
      case ((mid, date), iter) => iter.toList.sortWith(_.ts < _.ts).take(1)
    }
    value1
  }

  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  /**
   * TODO 利用Redis做跨批次去重
   */
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc:SparkContext) = {
    //TODO 方案一：使用filter算子
    val value1: DStream[StartUpLog] = startUpLogDStream.filter(log => {
      //a.获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.查询Redis中是否存在mid
      val exist: lang.Boolean = jedisClient.sismember(s"DAU:${log.logDate}", log.mid)
      //c.归还连接
      jedisClient.close()
      //d.返回结果
      !exist
    })
    //TODO 方案二：使用分区操作代替单条数据操作
//    val value2: DStream[StartUpLog] = startUpLogDStream.mapPartitions(iter => {
//      //a.获取连接
//      val jedisClient: Jedis = RedisUtil.getJedisClient
//      //b.遍历iter,过滤数据
//      val filterLogs: Iterator[StartUpLog] = iter.filter(log => {
//        !jedisClient.sismember(s"DAU:${log.logDate}", log.mid)
//      })
//      //c.归还连接
//      jedisClient.close()
//      //d.返回结果
//      filterLogs
//    })
   // value1
  // value2
    //TODO 方案三：
    val value3: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //获取Redis数据并广播
      //a.获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.查询数据
      val mids: util.Set[String] = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")
      //c.归还连接
      jedisClient.close()
      //d.广播
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)
      //对RDD操作，进行去重
      rdd.filter(log => {
        !midBC.value.contains(log.mid)
      })
    })
    value3
 }
  /**
   * 将两次去重之后的结果中的Mid写入Redis,给当天的后置批次去重使用
   */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {

    startUpLogDStream.foreachRDD(rdd=>{
        //TODO 使用分区操作代替单条数据操作
      rdd.foreachPartition(iter=>{
        //TODO 获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //TODO 遍历iter,写库
        iter.foreach(startLog=>{
          val redisKey=s"DAU:${startLog.logDate}"
          jedisClient.sadd(redisKey,startLog.mid)
        })
        //归还连接
        jedisClient.close()
      })
    })
  }

}
