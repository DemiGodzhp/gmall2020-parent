package com.atguigu.gmall.realtime.bean

/**
 * @author Demigod_zhang
 * @create 2020-10-27 21:14
 */
case class UserStatus(
                       userId:String,  //用户id
                       ifConsumed:String //是否消费过   0首单   1非首单
                     )
