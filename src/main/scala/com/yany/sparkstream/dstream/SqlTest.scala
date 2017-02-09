package com.yany.sparkstream.dstream

import com.yany.sparkstream.datasource.DataSource
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yanyong on 2017/2/9.
  */
object SqlTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AppStreamStart").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(4))
    val stream = DataSource.getTcpSource(ssc)

    val words = stream.flatMap(_.split(" "))
    words.foreachRDD(rdd => {
      /** Get the singleton instance of SparkSession */
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val wordsDataFrame = rdd.toDF("word")

      // Create a temporary view
      wordsDataFrame.createOrReplaceTempView("words")

      val wordCountsDataFrame =
        spark.sql("select word, count(*) as total from words group by word")
      wordCountsDataFrame.show()
    })

    ssc.start()
    ssc.awaitTermination()

  }

}
