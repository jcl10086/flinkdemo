package org.jcl.util

import java.util

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Created by admin on 2018/9/19.
  */
class CountSumTest extends RichFlatMapFunction[(String,Int),(String,Int)] with ListCheckpointed[(String,Int)]{

//  var currentState: ValueState[(String,Int)]= _

  var currentValue: (String,Int)= _


//  override def open(config : Configuration) : Unit = {
//
//    val descriptor=new ValueStateDescriptor[(String,Int)]("hello",createTypeInformation[(String,Int)])
//
//    currentValue = getRuntimeContext.getState(descriptor).value()
//
//  }

  override def flatMap(in: (String, Int), collector: Collector[(String, Int)]): Unit = {

//    var stateValue= currentState.value()

    if(currentValue == null){

      currentValue = (in._1,0)

    }

//    currentState.update(in._1,stateValue._2+in._2)

    currentValue=(in._1,currentValue._2+in._2)

    collector.collect(currentValue)

//    state.clear()

  }

  override def restoreState(value: util.List[(String, Int)]): Unit = {

    currentValue=value.get(0)

  }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[(String, Int)] = {

    util.Collections.singletonList(currentValue)

  }
}
