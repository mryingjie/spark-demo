package com.demo.spark.core.exercise

import java.time.{Instant, LocalTime, ZoneId}
import java.time.format.DateTimeFormatter

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.junit._

import scala.collection.mutable

/**
  * create by ZhengYingjie on 2019/3/12 8:57
  */
class Exercise {

  val conf: SparkConf = new SparkConf()
  var context: SparkContext = _

  //  * 格式 ：timestamp  province   city     userid   adid
  //  *       某个时间点  某个省份 某个城市 某个用户 某个广告
  //
  //  * 用户ID范围: 0 - 99
  //  * 省份、城市 ID相同 ： 0 - 9
  //  * adid: 0 - 19
  //
  //  需求：统计每一个省份点击TOP3的广告ID
  //  Map(proId->List((广告Id，点击数),(广告Id，点击数),(广告Id，点击数)))

  //  需求：统计每一个省份每一个小时的TOP3广告的ID
  //    Map(省份,Map(小时,List(广告id,点击数),(广告id,点击数),(广告id,点击数)))
  @Test
  def test02(): Unit = {
    val result = context.textFile("agent.log")
      .map {
        //划分到最小粒度（省份：小时数：广告id，点击数）
        line => {
          val strings = line.split(" ")
//          val hour = new DateTime(1516609241720L).getHourOfDay
          val hour = Instant.ofEpochMilli(strings(0).toLong)
            .atZone(ZoneId.of("Asia/Shanghai"))
            .toLocalTime
            .getHour
          ((strings(1) + ":" + hour + ":" + strings(4)), 1)
        }
      }.reduceByKey(_ + _) //（省份：小时数：广告id，点击数）
      //放大粒度(省份:小时数,List((广告id，点击数),(广告id，点击数)))
      .map {
        item => {
          val strings = item._1.split(":")
          ((strings(0) + ":" + strings(1)), ((strings(2), item._2)))
        }
      }.groupByKey()
      .map {
            //再放大粒度 （省份,（小时,List（(广告id，点击数)）））
        item => {
          val proAndHour = item._1.split(":")
          val adIdAndNums = item._2.toList.sortWith((x, y) => x._2 > y._2).take(3)
          (proAndHour(0),(proAndHour(1),adIdAndNums))
        }
      }.collect()
      val outMap = mutable.Map[String,mutable.Map[String,List[(String,Int)]]]()
      for(item <- result){
        var innerMap: mutable.Map[String, List[(String, Int)]] = null
        if(outMap.contains(item._1)){
          innerMap = outMap.get(item._1).get
        }else{
          innerMap = new mutable.HashMap[String, List[((String, Int))]]()
        }
        val value = item._2
        innerMap += (value._1->value._2)
        outMap += (item._1->innerMap)
      }
      println(result)

  }


  @Test
  def test01(): Unit = {

    val result = context.textFile("agent.log")
      //    1516609143867|6|7|64|16
      //    1516609143869|9|4|75|18
      //    1516609143869|1|7|87|12
      //    1516609143869|2|8|92|9
      .map(_.split(" "))

      //划分到最小粒度   省份:广告  点击数
      //                        6:16        1
      .map(x => (x(1) + ":" + x(4), 1))
      .reduceByKey(_ + _)
      //放大粒度 (省份Id，(广告Id，点击数))
      .map {
      item => {
        val strings = item._1.split(":")
        val proId = strings(0)
        val adId = strings(1)
        (proId, (adId, item._2))
      }
    }.groupByKey() //按省份分组 (省份Id,List((广告Id，点击数),(广告Id，点击数)))
      .map {
      item => {
        //组内排序
        val adIdAndNums = item._2
        val adIdAndNumsSortRes = adIdAndNums.toList.sortWith {
          (x, y) => x._2 > y._2
        }
        Map((item._1, adIdAndNumsSortRes.take(3))) //取前三个 并转成map
      }
    }.collect()
    println(result.mkString("|"))


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
