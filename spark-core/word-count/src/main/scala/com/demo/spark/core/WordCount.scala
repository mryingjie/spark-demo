package com.demo.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * create by ZhengYingjie on 2019/3/9 11:50
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    //新建sparkConf对象
    val conf = new SparkConf().setAppName("wordCount")
//      .setMaster("local[*]")

    //创建sparkContext
    val context = new SparkContext(conf)

    //读取数据 统计
    val words = context.textFile("RELEASE")

    val res = words.flatMap(x =>x.split(" ")).map((_,1)).reduceByKey(_+_).collect()

    res.foreach(println _)

    //关闭连接
    context.stop()
  }

}
