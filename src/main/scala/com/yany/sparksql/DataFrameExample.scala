package com.yany.sparksql

/**
  * Created by yanyong on 2017/2/9.
  */
object DataFrameExample {

  def main(args: Array[String]): Unit = {

    val spark = new SparkSqlInit().initSparkSession()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val df = spark.read.json("src/main/resources/people.json")

    df.show()

    // Print the schema in a tree format
    df.printSchema()

    // Select only the "name" column
    df.select("name").show()

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()

    // Select people older than 21
    df.filter($"age" > 21).show()

    // Count people by age
    df.groupBy("age").count().show()

    //==============================================

    /** Running SQL Queries Programmatically */
    println("Running SQL Queries Programmatically")

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()
//    sqlDF.write.parquet("src/main/resources/people")

    //==============================================

    /** Global Temporary View
      *
      * Temporary views in Spark SQL are session-scoped and will disappear if the session that creates it terminates.
      *
      * If you want to have a temporary view that is shared among all sessions and keep alive until the Spark application terminates, you can create a global temporary view.
      *
      * Global temporary view is tied to a system preserved database global_temp, and we must use the qualified name to refer it
      * */

    println("Global Temporary View")

    // Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    spark.sql("SELECT * FROM global_temp.people").show()

    // Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()



    //==============================================
    /**
      * Inferring the Schema Using Reflection
      */
    println("Inferring the Schema Using Reflection")

    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val peopleDF = spark.sparkContext
      .textFile("src/main/resources/people.txt")
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()

    peopleDF.createOrReplaceTempView("people")

    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    // or by field name
    teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()



  }

}
