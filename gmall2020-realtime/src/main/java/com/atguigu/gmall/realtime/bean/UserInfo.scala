package com.atguigu.gmall.realtime.bean

/**
 * @author Demigod_zhang
 * @create 2020-10-28 18:33
 */
case class UserInfo(
                     id:String,
                     user_level:String,
                     birthday:String,
                     gender:String,
                     var age_group:String,//年龄段
                     var gender_name:String
                   )
