package org.jcl.util

import java.sql.{Connection, DriverManager}
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * Created by admin on 2018/10/23.
  */
class MysqlSink extends RichSinkFunction[String]{

  var connection:Connection = _

  override def close(): Unit = {
    super.close()

    connection.close()
  }

  override def open(parameters: Configuration): Unit = {

    super.open(parameters)

    Class.forName("com.mysql.jdbc.Driver")

    val url = "jdbc:mysql://192.168.2.200:3306/test?user=root&password=Touch-Spring.com&useUnicode=true&characterEncoding=UTF8";

    connection = DriverManager.getConnection(url)

  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {

//    super.invoke(value, context)

    val sql="insert into kafka_data(id,msg) values(?,?)"

    val ps=connection.prepareStatement(sql)

    val id =new Date().getTime.toString

    ps.setString(1,id)

    ps.setString(2,value)

    ps.execute()

    ps.close()
  }
}
