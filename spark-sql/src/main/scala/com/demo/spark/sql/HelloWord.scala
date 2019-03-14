package com.demo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * create by ZhengYingjie on 2019/3/12 19:59
  */
object HelloWord {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("spark-sql").setMaster("local[*]")

    val spark = SparkSession.builder.config(conf).getOrCreate()

    val sc = spark.sparkContext


    val df = spark.read.json("spark-sql/target/classes/doc/emoloyee.json");
    //展示整个表
//    df.show;

    //表结构
//    df.printSchema();

    //DSL风格查询
    import spark.implicits._
    df.filter($"salary" > 3300).show


      //sql风格
    df.createOrReplaceTempView("employee")

    val frame = spark.sql("select * from employee where salary > 3300")
    frame.show()
    frame.write.json("E:\\demo\\spark-demo\\spark-sql\\abc.json")

    spark.close()
  }

}
