package com.demo.spark.sql.transformation

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{After, Test}

/**
  * create by ZhengYingjie on 2019/3/13 9:29
  */
class TransformationClient {

  val conf: SparkConf = new SparkConf().setAppName("transformation").setMaster("local[*]")
  val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
  val sc: SparkContext = spark.sparkContext

  import spark.implicits._


  /**
    * RDD和DataFrame之间的转换
    *
    */
  @Test
  def testRDDAndDataFrame(): Unit = {

    //RDD==>DataFrame
    val rdd = sc.textFile("E:\\demo\\spark-demo\\spark-sql\\src\\main\\resources\\doc\\people.txt")
    //1 .转为元组 然后toDF（列名1，列名2....）
    val res1 = rdd map {
      item => {
        val strings = item.split(",")
        (strings(0).trim, strings(1))
      }
    }
    val frame = res1.toDF("name", "age")
    frame.show

    //2.反射  需要样例类  和转化为DS差不多
    //RDD==>DataSet
    val rdd3 = sc.textFile("E:\\demo\\spark-demo\\spark-sql\\src\\main\\resources\\doc\\people.txt")
    val res2 = rdd3.map {
      x => {
        val pa = x.split(",");
        People(pa(0), pa(1).toInt)
      }
    }
    val dsPeople = res2.toDF()
//    val dsPeople = res2.toDS()

    //3.编程方式  跟原生jdbc类似  列数类型等都不知道
    val schemaStr = "name,age"
    val fields = schemaStr.split(",")
      .map(fieldName =>StructField(fieldName,StringType,nullable = true))
    val schema = StructType(fields)
    val rowRdd = rdd.map(_.split(",")).map(attr=>Row(attr(0).trim,attr(1).trim))
    val df = spark.createDataFrame(rowRdd,schema)

    //DataFrame==>RDD
    val rdd2 = frame.rdd.map(_.getAs[String]("name"))
    //    val rdd2 = frame.rdd.map(_.getString(0))

  }

  /**
    * DataFrame和DataSet之间的转换
    */
  @Test
  def testDataFrameAndDataSet(): Unit = {

    //DataSet=>DataFrame
    val ds1 = spark.createDataset(List(1,23,4,2))
    val df = ds1.toDF("value")

    //DataFrame => DataSet
    //需要样例类 如果是对象  类的属性名称要和DataFrame的列名一致
    val ds2 = df.as[Int]
//    val context = ds2.sqlContext
//    context.sql()
  }

  /**
    * RDD和DataSet之间的转化
    * 需要样例类
    */
  case class People(name: String, age: Int)
  @Test
  def testRDDAndDataSet(): Unit = {
    //RDD==>DataSet
    val rdd = sc.textFile("E:\\demo\\spark-demo\\spark-sql\\src\\main\\resources\\doc\\people.txt")
    val res = rdd.map {
      x => {
        val pa = x.split(",");
        People(pa(0), pa(1).toInt)
      }
    }
    val dsPeople = res.toDS()


    //DataSet => RDD
//    val rdd2 = dsPeople.rdd
  }



  @After
  def end = {
    sc.stop()
    spark.close()
  }


}

