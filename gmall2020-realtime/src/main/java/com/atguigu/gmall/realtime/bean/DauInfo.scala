package com.atguigu.gmall.realtime.bean

/**
 * @author Demigod_zhang
 * @create 2020-10-23 19:18
 */
case class DauInfo(  mid:String,//设备id
                     uid:String,//用户id
                     ar:String,//地区
                     ch:String,//渠道
                     vc:String,//版本
                     var dt:String,//日期
                     var hr:String,//小时
                     var mi:String,//分钟
                     ts:Long //时间戳
                  ){}
