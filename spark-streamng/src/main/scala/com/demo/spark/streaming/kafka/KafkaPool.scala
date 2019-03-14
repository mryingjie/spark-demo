package com.demo.spark.streaming.kafka

import java.util.Properties

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.serialization.StringSerializer

/**
  * create by ZhengYingjie on 2019/3/14 16:26
  * kafka连接池
  */

class kafkaProxyFactory(brokers:String) extends BasePooledObjectFactory[KafkaProxy] {
  //创建实例
  override def create(): KafkaProxy = {
    new KafkaProxy(brokers)
  }

  override def wrap(t: KafkaProxy): PooledObject[KafkaProxy] = {
    new DefaultPooledObject[KafkaProxy](t)
  }
}


object KafkaPool {

  //声明一个连接池对象
  private var kafkaProxyPool:GenericObjectPool[KafkaProxy] =_

  def apply(brokers:String): GenericObjectPool[KafkaProxy] = {
    if(kafkaProxyPool == null){
      KafkaPool.synchronized{
        if(null == kafkaProxyPool){
          kafkaProxyPool = new GenericObjectPool[KafkaProxy](new kafkaProxyFactory(brokers))
        }
      }
    }
    kafkaProxyPool
  }


}

class KafkaProxy(brokers:String){

  private val props:Properties = new Properties()

  /**
    * key.serializer.class 默认为Serializable.class
    */
  //        props.put("serializer.class", "kafka.serializer.StringDecoder");
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName());
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,classOf[StringSerializer].getName());


  /**
    * kafak broker对应的主机
    */
  //        props.put("metadate.broker.list", "hadoop.abc6.net:9092,zhengyingjie2.abc6.net:9092,zhengyingjie3.abc6.net:9092");
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

  /**
    * request.required.acks 设置发送数据是否需要服务器的反馈 三个值0 1 -1
    * 0 表示producer永远不会等待一个来自broker的ack
    * 1 表示在leader replica收到数据后 就会返回ack
    * 但是如果刚写到leader 还没写到replica上就挂掉了 数据可能丢失
    * -1 表示所有的副本都收到数据了才会返回ack
    *
    * 默认
    */
  props.put(ProducerConfig.ACKS_CONFIG, "1");


  /**
    * 分区配置 默认是org.apache.kafka.clients.producer.internals.DefaultPartitioner
    * 可以自定义
    */
  props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, classOf[DefaultPartitioner].getName());

  private val kafkaConn = new KafkaProducer[String,String](props)

  def send(topic:String,key:String,value:String): Unit ={
    val  producerRecord = new ProducerRecord(topic, null, null, key, value, null);

    kafkaConn.send(producerRecord, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        println("topic:"+metadata.topic(),"  partition:"+metadata.partition(),"  offset:"+metadata.offset())
      }
    })


  }

  def close(): Unit ={
    kafkaConn.close()
  }

}