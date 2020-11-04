package com.atguigu.gmall.realtime.dim

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.realtime.bean.UserInfo
import com.atguigu.gmall.realtime.utils.{MyKafkaUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Demigod_zhang
 * @create 2020-10-28 19:34
 */
object UserInfoApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("UserInfoApp").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic = "ods_user_info";
    val groupId = "gmall_user_info_group"


    val offset : Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

    var recordDstream: InputDStream[ConsumerRecord[String, String]] = null
    if(offset.size>1 && offset != null) {
      recordDstream=MyKafkaUtil.getKafkaStream(topic,ssc,offset,groupId)
    }else{
      recordDstream=MyKafkaUtil.getKafkaStream(topic,ssc)
    }
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = recordDstream.transform {
      rdd =>{
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    val userInfoDstream: DStream[UserInfo] = inputGetOffsetDstream.map {
      record =>{
        val userInfoJsonStr: String = record.value()
        val userInfo: UserInfo = JSON.parseObject(userInfoJsonStr, classOf[UserInfo])

        //把生日转成年龄
        val formattor = new SimpleDateFormat("yyyy-MM-dd")
        val date: Date = formattor.parse(userInfo.birthday)
        val curTs: Long = System.currentTimeMillis()
        val  betweenMs = curTs - date.getTime
        val age = betweenMs/1000L/60L/60L/24L/365L
        if(age<20){
          userInfo.age_group="20岁及以下"
        }else if(age>30){
          userInfo.age_group="30岁以上"
        }else{
          userInfo.age_group="21岁到30岁"
        }
        if(userInfo.gender=="M"){
          userInfo.gender_name="男"
        }else{
          userInfo.gender_name="女"
        }
        userInfo
      }
    }

    userInfoDstream.foreachRDD{
      rdd=>{
        import org.apache.phoenix.spark._
        rdd.saveToPhoenix(
          "GMALL0523_USER_INFO",
          Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER","AGE_GROUP","GENDER_NAME")
          ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181")
        )
        OffsetManagerUtil.saveOffset(groupId, topic, offsetRanges)
      }

    }

    ssc.start()
    ssc.awaitTermination()



  }
}
