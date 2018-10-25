package org.jcl.util

import java.util

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

/**
  * Created by admin on 2018/9/19.
  */
class CountSum extends RichFlatMapFunction[(String,Int),(String,Int)]{

  var currentState: ValueState[(String,Int)]= _


  override def open(config : Configuration) : Unit = {

    val descriptor=new ValueStateDescriptor[(String,Int)]("hello",createTypeInformation[(String,Int)])

    currentState = getRuntimeContext.getState(descriptor)

  }

  override def flatMap(in: (String, Int), collector: Collector[(String, Int)]): Unit = {

    var stateValue= currentState.value()

    if(stateValue == null){

      stateValue = (in._1,0)

    }

    currentState.update(in._1,stateValue._2+in._2)

    stateValue=(in._1,stateValue._2+in._2)

    collector.collect(stateValue)

//    state.clear()

  }

}
