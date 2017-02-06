package com.yany.sparkstream

import com.yany.sparkstream.datasource.DataSource
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yanyong on 2017/2/5.
  */
object AppStreamStart {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AppStreamStart").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))

    val streamTCP = DataSource.getTcpSource(ssc)

    val streamKafka = DataSource.getKafkaSource(ssc)
    val streamKafka_map = streamKafka.map(_._2)

    // 合并多个输入流 当接收有瓶颈是可多建几个输入流
    val stream = streamTCP.union(streamKafka_map)

    stream.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

}
