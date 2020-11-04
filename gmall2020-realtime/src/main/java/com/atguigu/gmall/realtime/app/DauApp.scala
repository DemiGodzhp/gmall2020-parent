package com.atguigu.gmall.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall.realtime.bean.DauInfo
import com.atguigu.gmall.realtime.utils._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

/**
 * @author Demigod_zhang
 * @create 2020-10-21 20:47
 */
object DauApp {
  def main(args: Array[String]): Unit = {

    //todo 创建json
    val conf: SparkConf = new SparkConf().setAppName("aaa").setMaster("local[4]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val groupId = "gmall_dau_0523"
    val topic = "gmall_start_0523"


    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!=null && offsetMap.size >0){

      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{

      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }


    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {

        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    val jsonObjDStream: DStream[JSONObject] = recordDstream.map {
      record =>
        val jsonStr: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        val ts: lang.Long = jsonObj.getLong("ts")
        val dateHourString: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
        val dateHour: Array[String] = dateHourString.split(" ")
        jsonObj.put("dt", dateHour(0))
        jsonObj.put("hr", dateHour(1))
        jsonObj

    }
    //jsonObjDStream.print()


    //todo redis 分区去重
    val filteredDStream: DStream[JSONObject] = jsonObjDStream.mapPartitions {
      jsonObjItr => {
        val jedisClient: Jedis = MyRedisUtil.getJedisClient
        val filteredList: ListBuffer[JSONObject] = new ListBuffer[JSONObject]
        for (jsonObj <- jsonObjItr) {

          val dt: String = jsonObj.getString("dt")

          val mid: String = jsonObj.getJSONObject("common").getString("mid")

          val dauKey: String = "dau:" + dt

          val isNew: lang.Long = jedisClient.sadd(dauKey, mid)
          if (jedisClient.ttl(dauKey) > 0) {
            jedisClient.expire(dauKey, 3600 * 24)
          }
          if (isNew == 1L) {
            filteredList.append(jsonObj)
          }
        }
        jedisClient.close()
        filteredList.toIterator

      }


    }
    //filteredDStream.count().print()


    filteredDStream.foreachRDD{
      rdd=>{
        //以分区为单位对数据进行处理
        rdd.foreachPartition{
          jsonObjItr=>{
            val dauInfoList: List[(String,DauInfo)] = jsonObjItr.map {
              jsonObj => {
                val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
                val dauInfo = DauInfo(
                  commonJsonObj.getString("mid"),
                  commonJsonObj.getString("uid"),
                  commonJsonObj.getString("ar"),
                  commonJsonObj.getString("ch"),
                  commonJsonObj.getString("vc"),
                  jsonObj.getString("dt"),
                  jsonObj.getString("hr"),
                  "00",
                  jsonObj.getLong("ts")
                )

                (dauInfo.mid,dauInfo)
              }
            }.toList

            //将数据批量的保存到ES中
            val dt: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
            MyESUtil.bulkInsert(dauInfoList,"gmall0523_dau_info_" + dt)
          }
        }
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }



}
