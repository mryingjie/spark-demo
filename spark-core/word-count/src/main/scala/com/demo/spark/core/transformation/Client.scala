package com.demo.spark.core.transformation

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.junit.{After, Before, Test}

/**
  * create by ZhengYingjie on 2019/3/11 11:16
  */
class Client {

  val conf:SparkConf = new SparkConf()
  var context:SparkContext = _

  /**
    * 三种处理方式得到的结果是一样的
    */
  @Test
  def testMap(): Unit ={
//    val res = context.parallelize(List(1,2,3,4,5,6,7,8)).map(_ + 1).collect()
//    val res = context.parallelize(List(1,2,3,4,5,6,7,8)).flatMap(x=>Array(x+1)).collect()

    //每个分区分别执行
//    val res = context.parallelize(List(1,2,3,4,5,6,7,8),2).mapPartitions(item => Iterator(item.map(_+1).mkString("|")))
    val res = context.parallelize(List(1,2,3,4,5,6,7,8),2)
                                  .mapPartitionsWithIndex((index,item)=>Iterator(index+":"+item.mkString("|")))
                                  .collect()
    res.foreach(println(_))

  }


  @Test
  def testMap2(): Unit ={
    val rdd = context.textFile("command.sh")
    val res = rdd.map(_.split(" ")).collect()
    res.foreach(item=>{
      println(","+item.mkString("|"))
    })

  }


  /**
    * 分为4个分区
    */
  @Test
  def testPartition(): Unit ={
    val rdd1 =
      context.parallelize(List(1,2,3,4,5,6,7,8))
        .map((_,null))
        .partitionBy(new HashPartitioner(4))
  }

  @Test
  def testReduceByKey(): Unit ={
    val rdd1 =
      context.parallelize(List(1,2,3,4,5,6,7,8,4,8)).map(x =>(x,1))
//        .reduce((x,y)=>(x._1+y._1,x._2+y._2))
          .groupByKey().map(x=>(x._1,x._2.reduceLeft(_+_)))
//      .reduceByKey((v1,v2)=>v1+v2).collect()

//     println(rdd1)
    rdd1.foreach(println(_))
  }

  /**
    * def combineByKey[C](
    * createCombiner: V => C,  作用在每一个分区，当第一次遇到某一个key时
    * mergeValue: (C, V) => C,  作用在每一个分区，当不是第一次遇到某个key时
    * mergeCombiners: (C, C) => C,  所有分区结果 将所有分区的最终结果合并
    * numPartitions: Int): RDD[(K, C)]
    * ("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)
    * (a,(88,1)),(b,(95,1)), ("a", 91), ("b", 93), ("a", 95), ("b", 98)
    * (a,(88+91,2)),(b,(95+93,2)),("a", 95), ("b", 98))
    * (a,(88+91+95),3),(b,(95+93+98),3)
    */
  @Test
  def testCombineByKey(): Unit ={
    //求每个人的平均分 a , b ,代表每个人
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
      .combineByKey(
        (v:Int) => (v,1),
        (x:(Int,Int),y:Int) =>(x._1+y,x._2+1),
        (a:(Int,Int),b:(Int,Int)) => (a._1+b._1,a._2+b._2)
      ).map{
      x => x match {
        case (name,(sum,num))=>(name,sum/num)
      }
    }.collect()

    println(res.mkString(","))
  }

  /**
    * def aggregateByKey[U: ClassTag](
    * zeroValue: U,
    * numPartitions: Int,
    * seqOp: (U, V) => U,
    * combOp: (U, U) => U: RDD[(K, U)]
    */
  @Test
  def testAggregateByKey(): Unit ={
    //求每个人的平均分 a , b ,代表每个人
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
      .aggregateByKey(
        (0,0),//初始值
        2//分区数
      )(
        (u:(Int,Int),v)=>(u._1+v,u._2+1),
        (x:(Int,Int),y:(Int,Int))=>(x._1+y._1,x._2+y._2)
      ).map{
        x => x match {
          case (name:String,(sum,num))=>(name,sum/num)
        }
      }.collect()

    println(res.mkString(","))

  }

  /**
    * aggregateByKey的简化版
    * def foldByKey(
    * zeroValue: V,
    * partitioner: Partitioner)
    * (func: (V, V) => V)  即两个函数均为同一个
    * : RDD[(K, V)]
    */
  @Test
  def testFoldByKey(): Unit ={
    //求每个人的平均分 a , b ,代表每个人
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
//      .foldByKey()
  }

  /**
    * def sortByKey(  按key排序
    * ascending: Boolean = true,升降序 默认为true升序
    * numPartitions:分区数
    * Int = self.partitions.length)
    * : RDD[(K, V)]
    */
  @Test
  def testSortByKey(): Unit ={
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
      .sortByKey(false).collect()
    println(res.mkString("|"))
  }


  /**
    * def sortBy[K](
    * f: (T) => K, key生成规则
    * ascending: Boolean = true, 升降序
    * numPartitions: Int = this.partitions.length)
    * (implicit ord: Ordering[K], ctag: ClassTag[K])比较规则
    * : RDD[T]
    */
  @Test
  def testSortBy(): Unit ={
    //按值升序排序
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
      .sortBy(_._2).collect()
    println(res.mkString("|"))

  }

  /**
    * 相当于 sql中的innerJoin
    */
  @Test
  def testJoin(): Unit ={
    val sc = context.makeRDD(Array(("a",2),("b",3)))
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
      .join(sc)
        .collect()
    println(res.mkString("|"))
  }

  /**
    * 两个RDD分别groupByKey  后 join
    */
  @Test
  def testCogroup(): Unit ={
    val sc = context.makeRDD(Array(("a",2),("b",3)))
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
      .cogroup(sc)
      .collect()
    println(res.mkString("|"))
  }


  /**
    * 笛卡尔积
    */
  @Test
  def testCartesian(): Unit ={
    val sc = context.makeRDD(Array(("a",2),("b",3)))
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
      .cartesian(sc)
      .collect()
    println(res.mkString("|"))
  }

  /**
    * 调用脚本  每个分区调用一次
    */
  @Test
  def testPipe(): Unit ={
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
      .pipe("command.sh").collect()
    println(res.mkString("|"))
  }

  /**
    * 修改分区数  shuffle true所有分区都重新分区  false只在一个excutor内重新分区
    */
  @Test
  def testCoalesce(): Unit ={
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
      .coalesce(1,false)
  }

  /**
    * 先分区后排序
    */
  @Test
  def testRepartitionAndSortWithinPartitions(): Unit ={
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
//      .repartitionAndSortWithinPartitions()
  }

  /**
    * 将每个分区的数据用Array包装返回后 合并为一个二维的数组
    */
  @Test
  def testGlom(): Unit ={
    val res = context.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)),2)
      .glom().collect()
    println(res.mkString("|"))
  }

  /**
    * 差集
    */
  @Test
  def testSubtract(): Unit ={
    val rdd1 = context.parallelize(List(1,2,3,4,5,6,7,8))

    val rdd2 = context.makeRDD(Array(1,2,4,5,20))
    val ints = rdd1.subtract(rdd2).collect()
    println(ints.mkString("|"))
  }

  /**
    * 交集
    */
  @Test
  def testIntersection(): Unit ={
    val rdd1 = context.parallelize(List(1,2,3,4,5,6,7,8))

    val rdd2 = context.makeRDD(Array(1,2,4,5,20))

    val res = rdd1.intersection(rdd2).distinct

    res.foreach(println(_))
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
