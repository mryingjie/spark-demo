package com.demo.spark.core.accumulator

import java.util

import org.apache.spark.util.AccumulatorV2

/**
  * create by ZhengYingjie on 2019/3/12 16:13
  */
class CustomAccu extends AccumulatorV2[String,java.util.Set[String]]{

  private val _logArray:java.util.Set[String] = new util.HashSet[String]()

  override def clone(): AnyRef = {
    val accu = new CustomAccu
    _logArray.synchronized{
      accu._logArray.addAll(this._logArray)
    }
    accu
  }


  //分区中的暂存对量是否为空
  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  //复制一个对象
  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val value = this.clone()
    value.asInstanceOf[AccumulatorV2[String, util.Set[String]]]
  }

  //重试你的暂存对象状态
  override def reset(): Unit = {
    _logArray.clear()
  }

  //累加方法
  override def add(v: String): Unit = {
    _logArray.add(v)

  }

  //将多个分区的累加器合并的方法
  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    other match {
      case x:CustomAccu => _logArray.addAll(x.value)
    }
  }

  //读取值
  override def value: util.Set[String] = {
    _logArray
  }
}
