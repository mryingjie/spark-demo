package com.demo.spark.streaming.transformation

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * create by ZhengYingjie on 2019/3/14 14:10
  */
object WordCount2 {

  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    //创建StreamingContext对象
    val ssc = new StreamingContext(conf,Seconds(2))

    //如果需要有状态 设置Chickpoint目录
    ssc.checkpoint("./checkpoint")


//    创建一个接收器来接受数据
    val linesDstream = ssc.socketTextStream("192.168.42.132",port=9999)

    //使用spark-core Rdd编程
//    val res = linesDstream.transform {
//      rdd => {
//        rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
//      }
//    }
//    res.print()

    //使用原生DStream编程
    val wordDStream = linesDstream.flatMap(_.split(" "))

    //将单词转换为kv结构
    val kvDStream = wordDStream.map((_,1))

    //合并 无状态
//    val result = kvDStream.reduceByKey(_+_)

    //合并 有状态
    val updateFunc = (values:Seq[Int],state:Option[Int])=> {
      Some(state.getOrElse(0)+values.sum)
    }
    val result =kvDStream.updateStateByKey[Int](updateFunc)
    result.print()

    ssc.start()
    println("服务启动成功！！！")
    ssc.awaitTermination()

  }

}
