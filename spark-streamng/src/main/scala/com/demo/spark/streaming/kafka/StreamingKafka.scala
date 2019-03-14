package com.demo.spark.streaming.kafka

import java.util.{Properties, UUID}

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * create by ZhengYingjie on 2019/3/14 15:18
  */
object StreamingKafka {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("kafkaStreaming").setMaster("local[*]")

    val ssc = new StreamingContext(conf, Seconds(3))

    //创建连接kafka的参数
    val brokerList = "192.168.42.132:9092"

    val zookeeperAddr = "192.168.42.132:2181"

    val sourceTopic = "source"

    val tagetTopic = "target"

    val groupid = "comsumer01"

    val properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName);
    //    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest");

    val kafkaParam = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.GROUP_ID_CONFIG -> groupid,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    //连接kafka
    val textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParam,
      Set(sourceTopic)
    )
    textKafkaDStream.map(
      item => s"key: ${item._1}\tvalue: ${item._2}"
    ).foreachRDD {
      item =>
        item.foreachPartition {
          ss => {
            //写回到kafka
            //创建一个kafka连接池
            val kafkaPool = KafkaPool(brokerList)
            //拿到连接
            val proxy = kafkaPool.borrowObject()
            //写数据
            for(s <- ss){
              println(s"接收到数据s:$s")
              proxy.send(tagetTopic,UUID.randomUUID().toString,s)
            }

            kafkaPool.returnObject(proxy)
          }
        }
    }
    ssc.start()
    println("spark-streaming server start!!!")
    ssc.awaitTermination()
  }


}
