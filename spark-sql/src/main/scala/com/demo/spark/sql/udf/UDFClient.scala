package com.demo.spark.sql.udf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.junit._

/**
  * create by ZhengYingjie on 2019/3/13 14:09
  */
class UDFClient extends Serializable {

  val conf: SparkConf = new SparkConf().setAppName("transformation").setMaster("local[*]")
  val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  /**
    * {"name":"张三","age":18,"salary":1993.9}
    * {"name":"李四","age":23,"salary":2132.8}
    * {"name":"王五","age":18,"salary":4342}
    * {"name":"赵六","age":22,"salary":8923.2}
    */
  @Test
  def testUDF1(): Unit ={
    val employeeDF = spark.read.json("E:\\demo\\spark-demo\\spark-sql\\src\\main\\resources\\doc\\emoloyee.json")

    employeeDF.createOrReplaceTempView("employee")

    //案例一 给每个名字后边加上一个前缀name
    //注册一个addName的UDF函数
    spark.udf.register("addName",(x:String)=>"name:"+x)
    val resDF = spark.sql("select addName(name) as name from employee where salary > 3000")
    resDF.show

  }


  /**
    * {"name":"a","classNo":1,"score":80}
    * {"name":"b","classNo":1,"score":78}
    * {"name":"c","classNo":1,"score":95}
    * {"name":"d","classNo":2,"score":74}
    * {"name":"e","classNo":2,"score":92}
    * {"name":"f","classNo":3,"score":99}
    * {"name":"g","classNo":3,"score":99}
    * {"name":"h","classNo":3,"score":45}
    * {"name":"i","classNo":3,"score":55}
    * {"name":"j","classNo":3,"score":78}
    *
    * 需求：求每个班级得分最高的学生的姓名+分数+班级
    *
    * 开窗函数
    */
  @Test
  def testUDF2(): Unit ={
    val employeeDF = spark.read.json("E:\\demo\\spark-demo\\spark-sql\\src\\main\\resources\\doc\\student.json")

    employeeDF.createOrReplaceTempView("student")

    spark.sql("select * from student").show()
    val frame = spark.sql("SELECT classNo,name,score,rank() over(PARTITION BY classNo ORDER BY score DESC) as rank from " +
      "student")
    frame.show()
    frame.createOrReplaceTempView("aaa")

    spark.sql("select * from aaa where rank = 1").show()

//    resDF.show
    //

  }

  /**
    * UDAF自定义聚合函数求工资平均值
    * {"name":"张三","age":18,"salary":1993}
    * {"name":"李四","age":23,"salary":2132}
    * {"name":"王五","age":18,"salary":4342}
    * {"name":"赵六","age":22,"salary":8923}
    */
  @Test
  def testUDAF3(): Unit ={
    val employeeDF = spark.read.json("E:\\demo\\spark-demo\\spark-sql\\src\\main\\resources\\doc\\emoloyee.json")

    employeeDF.createOrReplaceTempView("employee")

    //注册自定义函数
    spark.udf.register("m_avg",new AVG())

    spark.sql("select m_avg(salary) from employee").show

  }

  @After
  def end = {
    sc.stop()
    spark.close()
  }

}

class AVG extends UserDefinedAggregateFunction with Serializable {
  //聚合函数输入的数据类型
  override def inputSchema: StructType = StructType(StructField("salary",LongType)::Nil)

  //小范围聚合临时量的数据类型
  override def bufferSchema: StructType = StructType(StructField("sum",LongType)::StructField("count",LongType)::Nil)

  //返回值的类型
  override def dataType: DataType = DoubleType

  //幂等性
  override def deterministic: Boolean = true

  //初始化的数据结构
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //每个分区中更新数据结构
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  //将所有分区的数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算值
  override def evaluate(buffer: Row): Any = {
    if(buffer.getLong(1)==0L){
      return 0.00
    }
    (buffer.getLong(0) / buffer.getLong(1)).toDouble
  }
}
