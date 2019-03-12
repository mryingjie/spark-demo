package com.demo.spark.core.accumulator

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before,Test}

/**
  * create by ZhengYingjie on 2019/3/12 15:53
  * 累加器 只能在driver端读取 excutor修改
  *
  * 广播变量
  */
class AccuClient {

  val conf: SparkConf = new SparkConf()
  var context: SparkContext = _

  @Test
  def testWithAccu(): Unit ={
    val rdd = context.makeRDD(1 to 10,2)

    val sum = context.longAccumulator("sss")
    val accu = new CustomAccu
    //注册自定义累加器
    context.register(accu)
    //广播变量
    val broadcast = context.broadcast(Array(1,2,3,4,5))
    rdd.map{
      x => {
        broadcast .value
        sum.add(x)
        accu.add(x+"")
        x
      }
    }.collect()

    println("sum = "+sum.value) //sum = 55
    println("accu ="+ accu.value)
  }


  @Test
  def testNoAccu(): Unit ={
    val rdd = context.makeRDD(1 to 10,2)

    var sum = 0

    rdd.map{
      x => {
        sum += x
        x
      }
    }.collect()

    println("sum = "+sum) //sum = 0

  }


  @Before
  def init(): Unit = {
    conf.setAppName("transformation").setMaster("local[*]")
    context = new SparkContext(conf)
  }

  @After
  def end = {
    context.stop()
  }

}
