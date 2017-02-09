package com.yany.sparksql

import org.apache.spark.sql.SparkSession

/**
  * Created by yanyong on 2017/2/9.
  */
class SparkSqlInit {

  def initSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

  }

}
