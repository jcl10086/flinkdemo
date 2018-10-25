package org.jcl.stream

import org.apache.flink.streaming.api.scala._

/**
  * Created by admin on 2018/9/13.
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text=env.readTextFile("data/1.txt")

    val counts: DataStream[(String, Int)] = text.flatMap(x=>(x.split(" ")))
    .map((_,1))
    .keyBy(0)
    .sum(1)

    counts.print()

    env.execute("Streaming WordCount")
  }

}
