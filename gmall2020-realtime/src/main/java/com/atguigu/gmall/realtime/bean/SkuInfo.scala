package com.atguigu.gmall.realtime.bean

/**
 * @author Demigod_zhang
 * @create 2020-10-30 19:25
 */
case class SkuInfo(
                    id:String ,
                    spu_id:String ,
                    price:String ,
                    sku_name:String ,
                    tm_id:String ,
                    category3_id:String ,
                    create_time:String,

                    var category3_name:String,
                    var spu_name:String,
                    var tm_name:String
                  )
