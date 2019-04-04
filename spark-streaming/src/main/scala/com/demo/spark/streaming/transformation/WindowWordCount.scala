package com.demo.spark.streaming.transformation

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * create by ZhengYingjie on 2019/3/19 9:02
  */
object WindowWordCount {

  def main(args: Array[String]): Unit = {

    //创建SparkConf对象
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    //创建StreamingContext对象
    val ssc = new StreamingContext(conf,Seconds(2))

    //如果需要有状态 设置Chickpoint目录
    ssc.checkpoint("./checkpoint")


    //    创建一个接收器来接受数据
    val linesDstream = ssc.socketTextStream("localhost",port=9999)


    //使用原生DStream编程
    val wordDStream = linesDstream.flatMap(_.split(" "))

    //将单词转换为kv结构
    val kvDStream = wordDStream.map((_,1))

    //合并 无状态
    //    val result = kvDStream.reduceByKey(_+_)

    //合并 有状态
    // 窗口大小 为12s， 12/3 = 4  滑动步长 6S，   6/3 =2
//    val wordCounts =
//      kvDStream.reduceByKeyAndWindow(
//        (a:Int,b:Int) => (a + b),
//        Seconds(12),
//        Seconds(6)
//      )

    //优化后的窗口  减去不要的加上新添加的节省计算成本
    val wordCounts =
      kvDStream.reduceByKeyAndWindow(
        _ + _,
        _ - _ ,
        Seconds(12),
        Seconds(6)
      )

    wordCounts.print()

    ssc.start()
    println("服务启动成功！！！")
    ssc.awaitTermination()


  }


}
