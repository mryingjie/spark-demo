package com.demo.spark.core.inputOutput

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Before, Test}

/**
  * create by ZhengYingjie on 2019/3/12 15:50
  */
class InputDemo {

  val conf: SparkConf = new SparkConf()
  var context: SparkContext = _

  @Test
  def testHDFS(): Unit = {
    val rdd = context.textFile("hdfs://host:port")
    //    context.wholeTextFiles()  针对小文件
    rdd.saveAsTextFile("hdfs://host:port")

    val rdd2 = context.sequenceFile[String, Int]("hdfs://host:port")
    rdd2.saveAsSequenceFile("hdfs://host:port")

    val rdd3 = context.objectFile[Object]("hdfs://host:port")

    rdd3.saveAsObjectFile("hdfs://host:port")
  }

  @Test
  def testMysql(): Unit = {
    val rdd = new JdbcRDD(
      context,
      () => {
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance()
        DriverManager.getConnection("jdbc:mysql://localhost:3306/spark?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC", "root", "zheng")
      },
      "select * from user where id >= ? and id <= ?;",
      1,
      10,
      1,
      r => {
        val user = new User()
        user.name = r.getString(2)
        user.age = r.getInt(3)
        user
      }
    )
    val users = rdd.collect()
    println(users)
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
