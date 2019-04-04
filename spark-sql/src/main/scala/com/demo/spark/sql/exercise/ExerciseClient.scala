package com.demo.spark.sql.exercise

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.Test
import org.junit.After

/**
  * create by ZhengYingjie on 2019/3/14 9:08
  */
class ExerciseClient {

  val conf: SparkConf = new SparkConf().setAppName("transformation").setMaster("local[*]")
  val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  @Test
  def testClient(): Unit ={
    val rddTbData: RDD[(String, String, String, String, String, String, String, String, String, String)] = sc.textFile("E:\\demo\\spark-demo\\spark-sql\\src\\main\\resources\\doc\\tbDate.txt")
      .map(item => {
        val items = item.split(",")
        var res:Tuple10[String,String,String,String,String,String,String,String,String,String] =null
        try {
          res = (items(0), items(1), items(2), items(3), items(4), items(5), items(6), items(7), items(8), items(9))
        } catch {
          case e: Exception => System.err.println("脏数据，舍弃：" + items + e.getMessage)
        }
        res
      })
    val tbDate = rddTbData.toDF("dateid","years","theyear","mouth","day","weekday","week","quarter","period","halfmonth")
    tbDate.createOrReplaceTempView("tbDate")
    println("**************************tbDate************************")
    tbDate.show()


    val tbStockRDD = sc.textFile("E:\\demo\\spark-demo\\spark-sql\\target\\classes\\doc\\tbStock.txt")
      .map(
        item => {
          val items = item.split(",")
          (items(0), items(1), items(2))
        }
      )
    val tbStock = tbStockRDD.toDF("ordernumber","locationid","dateid")
    tbStock.createOrReplaceTempView("tbStock")
    println("**************************tbStock************************")
    tbStock.show()

    val tbStockDetailRDD = sc.textFile("E:\\demo\\spark-demo\\spark-sql\\target\\classes\\doc\\tbStockDetail.txt")
      .map(
        item => {
          val items = item.split(",")
          (items(0), items(1), items(2), items(3), items(4), items(5))
        }
      )
    val tbStockDetail = tbStockDetailRDD.toDF("ordernumber","rownum","itemid","number","price","amount")
    tbStockDetail.createOrReplaceTempView("tbStockDetail")
    println("**************************tbStockDetail************************")
    tbStockDetail.show()

//    计算所有订单中每年的销售单数、销售总额
//    println("*************计算所有订单中每年的销售单数、销售总额*********")
//    val res1 = spark.sql("SELECT a.theyear,sum(b.amount),count(DISTINCT b.ordernumber) \nfrom tbDate a join tbStock c" +
//      " on a" +
//      ".dateid = c.dateid\njoin  tbStockDetail b on b.ordernumber = c.ordernumber\ngroup by a.theyear   \norder by a" +
//      ".theyear")
//    res1.show()

    //计算所有订单每年最大金额订单的销售额
//    val res2 = spark.sql("select a.theyear,d.sumOfAmount,rank() over(PARTITION by a.theyear order by d.sumOfAmount DESC) rank\nfrom tbDate a join (SELECT\n\tc.dateid,\n\tb.ordernumber,\n\tsum(b.amount) AS sumOfAmount\nFROM\n\ttbStockDetail b\nJOIN tbStock c ON b.ordernumber = c.ordernumber\nGROUP BY\n\tb.ordernumber,c.dateid) d on a.dateid = d.dateid")
//    res2.createOrReplaceTempView("rescenter")
//    spark.sql("select * from rescenter where rank = 1").show()

    //计算所有订单中每年最畅销货品
    //目标：统计每年最畅销货品（哪个货品销售额amount在当年最高，哪个就是最畅销货品）

    //1.计算每个货品的每年销售总额
    val center1 = spark.sql("SELECT\n\ta.theyear,\n\tb.itemid,\n\tsum(b.amount) AS sumOfAmount\nFROM\n\ttbDate " +
      "a\nJOIN tbStock c ON" +
      " a.dateid = c.dateid\nJOIN tbStockDetail b ON b.ordernumber = c.ordernumber\nGROUP BY\n\ta.theyear,b.itemid")
    center1.createOrReplaceTempView("d")
    //2.对上一步的销售总额按年分组排序
    val center2 = spark.sql("SELECT\n\td.theyear,\n\td.itemid,\n\td.sumofAmount,\n\trank () over (\n\t\tPARTITION BY " +
      "d.theyear\n\t\tORDER BY\n\t\t\td.sumofAmount desc\n\t) rank \nFROM d")
    center2.createOrReplaceTempView("e")
    spark.sql("select * from e where rank = 1").show
  }


  @After
  def end = {
    sc.stop()
    spark.close()
  }

}
