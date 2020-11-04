package com.atguigu.gmall.realtime.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @author Demigod_zhang
 * @create 2020-10-21 19:45
 */
object MyPropertiesUtil {
  def main(args: Array[String]): Unit = {

    val properties: Properties = MyPropertiesUtil.load("config.properties")
    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName: String): Properties = {
      val prop = new Properties();
      prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.
        getResourceAsStream(propertieName), "UTF-8"))
      prop
  }



}
