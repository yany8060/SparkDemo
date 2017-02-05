package com.yany.sparkstream.datasource

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by yanyong on 2017/2/5.
  */
object DataSource {
  def getTcpSource(ssc: StreamingContext): DStream[String] = {
    new TcpSource().source(ssc)
  }

  def getKafkaSource(ssc: StreamingContext): InputDStream[(String, String)] = {
    new KafkaSource().source(ssc)
  }

}


/**
  * nc -lk 9999
  */
class TcpSource {
  def source(ssc: StreamingContext): DStream[String] = {
    ssc.socketTextStream("localhost", 9999)
  }
}

class KafkaSource {
  def source(ssc: StreamingContext): InputDStream[(String, String)] = {
    val zkQuorum = "10.211.55.5:2181"

    val groupId = "yany"
    val numThreads = 1
    val topics = Map("testYanY" -> numThreads.toInt)

    KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)

  }
}