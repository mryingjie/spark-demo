package com.demo.spark.core.exercise

import org.joda.time.DateTime


/**
  * create by ZhengYingjie on 2019/3/12 10:37
  */
object TestClient {

  def main(args: Array[String]): Unit = {

    val hour = new DateTime(1516609241720L).getHourOfDay

    println(hour)

  }

}
