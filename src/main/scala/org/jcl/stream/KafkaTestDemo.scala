package org.jcl.stream

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.jcl.util.RedisMapperUtils

/**
  * Created by admin on 2018/9/13.
  */
object KafkaTestDemo {

  def main(args: Array[String]): Unit = {

    val params =ParameterTool.fromPropertiesFile("data/config.properties");

    val conf = new FlinkJedisPoolConfig
      .Builder()
      .setHost("master")
      .setPassword("test")
      .setPort(6379).setDatabase(0)
      .build

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //每1秒检测一次的检查点
    env.enableCheckpointing(1000)

    env.setStateBackend(new FsStateBackend("file:///hello/checkpoints-data/"))

//    val config = env.getCheckpointConfig

//    conf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

//    config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    env.getCheckpointConfig.setCheckpointInterval(1000)

//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

//    env.setStateBackend(new FsStateBackend("hdfs://master:8020/checkpoints-data/"))



    val kafkaConsumer = new FlinkKafkaConsumer010(params.get("topic"),new SimpleStringSchema,params.getProperties)

    val messageStream = env
      .addSource(kafkaConsumer)
      .flatMap(x=> (x.split(" ")) )
      .map(x=> (x,1))
      .keyBy(0)
      .sum(1)
//      .flatMap(new CountSum)
      .map(x=> (x._1,x._2.toString) )

    messageStream.print()

    messageStream.addSink(new RedisSink[(String, String)](conf, new RedisMapperUtils))


//    val mysqlSink=new MysqlSink()
//
//    val dataStream = env
//      .addSource(kafkaConsumer)
//      .flatMap(x=>(x))
//      .map(x=>(x.toString))
//
//    dataStream.addSink(mysqlSink)


    env.execute("Kafka 0.10 Example")

  }

}
