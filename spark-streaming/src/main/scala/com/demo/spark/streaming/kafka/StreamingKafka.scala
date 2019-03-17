package com.demo.spark.streaming.kafka

import java.util.{Properties, UUID}

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
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

    // 1.直接连接kafka 如果kafka宕机可能会造成数据的丢失
//    val textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc,
//      kafkaParam,
//      Set(sourceTopic)
//    )



    //2.1 查看zk中是否有保存 没有就直接创建
    // 有的话a.需要读取zk中的offset
    // b.校准最小的offset
    //获取每个partition最小的offset
    // 如果最小的offset > zk中的offset 则将这个最小的offset定为最终的offset

//    创建连接到zookeeper查看收存在数据保存
    var textKafkaDStream:InputDStream[(String,String)] = null
    val topicDirs = new ZKGroupTopicDirs(groupid,sourceTopic);
    val zkTopicPath = topicDirs.consumerOffsetDir
    val zkClient = new ZkClient(zookeeperAddr);

    val countChildren = zkClient.countChildren(zkTopicPath)

    if(countChildren>0){
      //有保存
      //最终保存上一次的状态
      var fromOffsets:Map[TopicAndPartition,Long] = Map()

      //获取kafka集群的元信息
      val topicList  = List(sourceTopic)

      //创建一个连接
      val getLeaderConsumer = new SimpleConsumer("192.168.42.132",9092,100000,10000,"OffsetLookUp")

      //创建获取元信息的request
      val request = new TopicMetadataRequest(topicList,0)
      val response = getLeaderConsumer.send(request)

      //获取返回的元信息 并解出元信息
      val topicMetadataOption = response.topicsMetadata.headOption
      //partitions就是包含了每一个分区的主节点所在的主机名称
      val partitions = topicMetadataOption match {
        case Some(tm) => {
          tm.partitionsMetadata
            .map(pm => (pm.partitionId, pm.leader.get.host))
            .toMap[Int, String]
        }
        //Map(主节点的id,host)
        case None => Map[Int, String]()
      }
      getLeaderConsumer.close()

      //打印元数据信息
      println("partition info is :" + partitions)
      println("children info is:"+countChildren)

      //获取每一个分区最小的offset
      for(i <- 0 until countChildren){

        //先获取zk中第i个分区保存的offset
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")

        println(s"partition ${i} 目前的offset是: ${partitionOffset}")

        //获取第i个分区的最小offset
//        创建一个到第i个分区主分区所在的Host上的连接
        val consumerMin = new SimpleConsumer(partitions(i),9092,100000,100000,"getMinOffset")
        //创建一个请求
        val tp = TopicAndPartition(sourceTopic,i)
        val requestMin = OffsetRequest(Map(tp->PartitionOffsetRequestInfo(OffsetRequest.EarliestTime,1)))
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets
        consumerMin.close()

        //校准
        var nextOffset = partitionOffset.toLong
        if(curOffsets.length>0 && nextOffset < curOffsets.head){
          println(s"zk中保存的offset是：${nextOffset}  kafka分区中保存的最小offset是：${curOffsets.head}")
          nextOffset = curOffsets.head
        }

        fromOffsets += (tp -> nextOffset)
        println(s"partition ${i} 校准后的offset是: ${nextOffset}")
      }
      zkClient.close()

      println("从zk中恢复创建kafka连接！！")
      val messageHandler = (mmd:MessageAndMetadata[String,String])=>(mmd.topic,mmd.message())
      textKafkaDStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
        ssc,
        kafkaParam,
        fromOffsets,
        messageHandler
      )
    }else{
      //没有保存 直接创建
      println("直接创建kafka连接")
      textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParam,
        Set(sourceTopic)
      )
    }

    //注意：需要先拿到新读取进来的数据的offset 不要转换成为另一个Dstream后再去拿
    //获取offsetRange 并保存
    var offsetRanges = Array[OffsetRange]()

    val textKafkaDStream2 = textKafkaDStream.transform{
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }


    textKafkaDStream2.map(
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

        //将Kafka每个分区中读取的offset更新到zk
        val updateTopicDirs = new ZKGroupTopicDirs(groupid,sourceTopic)
        val updateZkClient = new ZkClient(zookeeperAddr)
        for(offset <- offsetRanges){
          println(s"Patition ${offset.partition} 保存到zk中的数据offset是：${offset.fromOffset.toString}")
          val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
          ZkUtils.updatePersistentPath(updateZkClient,zkPath,offset.fromOffset.toString)
        }
        updateZkClient.close()
    }
    ssc.start()
    println("spark-streaming server start!!!")
    ssc.awaitTermination()
  }


}
