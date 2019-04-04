package com.demo.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * create by ZhengYingjie on 2019/3/14 14:10
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    //创建StreamingContext对象
    val ssc = new StreamingContext(conf,Seconds(2))

//    创建一个接收器来接受数据
    val linesDstream = ssc.socketTextStream("localhost",port=9999)

    val wordDStream = linesDstream.flatMap(_.split(" "))

    //将单词转换为kv结构
    val kvDStream = wordDStream.map((_,1))

    //合并
    val result = kvDStream.reduceByKey(_+_)

    result.print()

    ssc.start()
    println("服务启动成功！！！")
    ssc.awaitTermination()
  }

}
