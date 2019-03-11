package com.demo.spark.core.creatRDD

import org.apache.spark.{SparkConf, SparkContext}

/**
  * create by ZhengYingjie on 2019/3/11 10:02
  * 从集合创建RDD
  */
object FromCollection {

  def main(args: Array[String]): Unit = {

    //新建sparkConf对象
    val conf = new SparkConf().setAppName("creatRDD")
          .setMaster("local[*]")

    //创建sparkContext
    val context = new SparkContext(conf)
    val ints = context.makeRDD(Array(1,2,3,4,5),4).collect()

    //以上代码相当于
//    context.parallelize(Array(1,2,3,4,5),4).collect()
    println(ints)
    //关闭连接
    context.stop()
  }

}
