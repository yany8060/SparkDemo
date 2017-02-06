package com.yany.sparkstream.dstream

import com.yany.sparkstream.datasource.DataSource
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by yanyong on 2017/2/5.
  */
object TransformTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AppStreamStart").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(3))
    val transformations = new Transformations()

    val stream = DataSource.getTcpSource(ssc)

    val mapResult = stream.map(new Transformations().customerMap(_))
    //    mapResult.print()

    val filterResult = mapResult.filter(new Transformations().customerFilter(_))
    //    filterResult.print()

    val flatMapResult = mapResult.flatMap(new Transformations().customerFlatMap(_))
    //    flatMapResult.print()

    val reduceByKeyResult1 = flatMapResult.map((_, 1)).reduceByKey(_ + _)
    val reduceByKeyResult = flatMapResult.map((_, "ss")).reduceByKey(new Transformations().customerReduceByKey(_, _))
    //    reduceByKeyResult.print()


    //union
//    val words = stream.flatMap(_.split(" "))
//    val wordsC = words.count()
//    wordsC.print()
//
//    val wordsOne = words.map(_ + "_one" )
//    val wordsTwo = words.map(_ + "_two" )
//    val unionWords = wordsOne.union(wordsTwo)
//    unionWords.print()

    // join 和 cogroupWords
    val words = stream.flatMap(_.split(" "))
    val wordsOne = words.map(word => (word, word + "_one"))
    val wordsTwo = words.map(word => (word, word + "_two"))
    val joinWords = wordsOne.join(wordsTwo)
    val cogroupWords = wordsOne.cogroup(wordsTwo)
    //    joinWords.print()
    cogroupWords.print()


    //    val reduceResult = flatMapResult.reduce(new Transformations().customerReduce(_, _));
    //    val reduceResult = flatMapResult.reduce((x, y) => new Transformations().customerReduce(x, y));
    //    reduceResult.print()

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }


}
