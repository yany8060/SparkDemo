package com.yany.sparkstream.dstream

import com.yany.sparkstream.datasource.DataSource
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yanyong on 2017/2/9.
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AppStreamStart").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val stream = DataSource.getTcpSource(ssc)

    ssc.checkpoint("/tmp/sparkCheckPoint")
    stream.checkpoint(Seconds(8))

    /** 默认1s  slideDuration
      * window length - The duration of the window
      * sliding interval - The interval at which the window operation is performed
      * */
    val windowResult = stream.window(Seconds(4))
    //    val windowResult = stream.window(Seconds(4), Seconds(2))
    //    windowResult.print()

    val countByWindowResult = stream.countByWindow(Seconds(4), Seconds(2))
    //    countByWindowResult.print();

    val mapResult = stream.flatMap(_.split(" "))
    val reduceByWindowResult = mapResult.reduceByWindow(new CustomerFunc().customerReduce(_, _), Seconds(4), Seconds(1))
//    reduceByWindowResult.print()

    val mapResult2 = stream.map((_, "s"));
    val reduceByKeyAndWindowReslut = mapResult2.reduceByKeyAndWindow(new CustomerFunc().customerReduceByKey(_, _), Seconds(4), Seconds(1))
    reduceByKeyAndWindowReslut.print()



    ssc.start()
    ssc.awaitTermination()
  }

}
