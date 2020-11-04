package com.atguigu.gmall.realtime.utils
import java.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
 * @author Demigod_zhang
 * @create 2020-10-23 19:39
 */
object OffsetManagerUtil {

  // type:hash   key: offset:topic:groupId   field:partition   value: 偏移量
  def getOffset(topic:String,groupId:String): Map[TopicPartition,Long]={
    val jedis: Jedis = MyRedisUtil.getJedisClient
    var offsetKey = "offset:" + topic + ":" + groupId
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    jedis.close()
    import scala.collection.JavaConverters._
    val oMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        println("读取偏移量：" + partition + ":" + offset)

        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap
    oMap
  }

  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {

    var offsetKey = "offset:" + topic + ":" + groupId

    val offsetMap: util.HashMap[String, String] = new util.HashMap[String,String]()

    for (offsetRange <- offsetRanges) {
      val partitionId: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset
      offsetMap.put(partitionId.toString,untilOffset.toString)
      println("保存偏移量" + partitionId + ":" + fromOffset + "----->" + untilOffset)
    }
    val jedis: Jedis = MyRedisUtil.getJedisClient
    jedis.hmset(offsetKey,offsetMap)
    jedis.close()
  }

}
