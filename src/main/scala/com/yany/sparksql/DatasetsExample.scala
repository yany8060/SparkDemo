package com.yany.sparksql

/**
  *
  *
  * Created by yanyong on 2017/2/9.
  */
case class Person(name: String, age: Long)

object DatasetsExample {

  val spark = new SparkSqlInit().initSparkSession()

  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._

  def main(args: Array[String]): Unit = {

    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()


    val primitiveDS = Seq(1, 2, 3).toDS()
    val ret = primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)
    println(ret.foreach(println(_)))

  }


}
