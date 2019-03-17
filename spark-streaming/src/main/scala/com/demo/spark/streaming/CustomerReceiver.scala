package com.demo.spark.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
  * create by ZhengYingjie on 2019/3/15 15:49
  */
object CustomerReceiver{
  def main(args: Array[String]): Unit = {

    //创建SparkConf对象
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")

    //创建StreamingContext对象
    val ssc = new StreamingContext(conf,Seconds(5))

    //    创建一个接收器来接受数据
    val linesDstream = ssc.receiverStream(new CustomerReceiver("192.168.42.132",port = 9999))

    val wordDStream = linesDstream.flatMap(_.split(" "))

    //将单词转换为kv结构
    val kvDStream = wordDStream.map((_,1))

    //合并
    val result = kvDStream.reduceByKey(_+_)

    result.print()

    ssc.start()
    println("服务启动成功！！！")
    ssc.awaitTermination()


  }
}

class CustomerReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  //传统的socket通信

  //接收器启动的时候
  override def onStart(): Unit = {
    new Thread("socket Receiver"){
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def receive(): Unit ={
    //创建socket练剑
    var socket:Socket = null
    var input:String = null

    try{
      socket = new Socket(host,port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))
      input = reader.readLine()

      while(!isStopped() && input!=null){
        //将数据提交给框架
        store(input)
        input = reader.readLine()
      }

      reader.close()
      socket.close()

      //重启
      restart("restart")

    }catch{
      case e:ConnectException =>restart("restart")
      case t:Throwable => restart("restart")
    }



  }

  //接收器停止的时候 主要做资源的销毁
  override def onStop(): Unit = ???
}
