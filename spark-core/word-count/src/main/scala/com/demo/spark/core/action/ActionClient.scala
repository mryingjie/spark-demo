package com.demo.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before,Test}

/**
  * create by ZhengYingjie on 2019/3/11 18:53
  */
class ActionClient {


  val conf:SparkConf = new SparkConf()
  var context:SparkContext = _

  /**
    * 相当于scala的reduce
    */
  @Test
  def testReduce(): Unit ={
    val rdd1 =
      context.parallelize(List(1,2,3,4,5,6,7,8))

    rdd1.count() //rdd中的元素个数
    rdd1.first()//
    rdd1.take(2)//前几个数据
    rdd1.takeOrdered(2)//前几个排序

  }

  /**
    *
    */
  @Test
  def testSaveAsTextFile(): Unit ={
    val rdd1 =
      context.parallelize(List(1,2,3,4,5,6,7,8))

  }


  /**
    * 统计每个key出现几次
    */
  @Test
  def testCountByKey(): Unit ={
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
      .countByKey()
    println(res.mkString("|"))
  }




  @Before
  def init(): Unit ={
    conf.setAppName("transformation").setMaster("local[*]")
    context = new SparkContext(conf)
  }

  @After
  def end = {
    context.stop()
  }

}
