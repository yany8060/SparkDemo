package com.yany.sparkstream.dstream

import scala.collection.mutable.ListBuffer

/**
  * Created by yanyong on 2017/2/5.
  */
class CustomerFunc {

  def customerMap(line: String): String = {
    println("customeMap:" + line)
    line + "ss"
  }

  /**
    * 返回类型必须是要 Boolean
    *
    * @param line
    * @return
    */
  def customerFilter(line: String): Boolean = {
    println("customeFilter:" + line)
    if (line.contains("a")) {
      false
    } else {
      true
    }

  }

  /**
    * 返回类型不定 可以是List[String] 或者Array[String] 等。。。
    *
    * @param line
    * @return
    */
  def customerFlatMap(line: String): Array[String] = {
    println("customeFlatMap:" + line)

    val strS = line.split(" ")
    for (i <- 0 to strS.length - 1) {
      strS(i) = strS(i) + "Y"
    }
    strS
  }


  def customerReduce(line1: String, line2: String): String = {
    line1 + "|||" + line2
  }

  /**
    *
    * reducebykey 通过对于两两的value进行操作,可自定义
    * @param line1
    * @param line2
    * @return
    */
  def customerReduceByKey(line1: String, line2: String): String = {
    "ss"
  }

  /**
    *
    * @param line
    * @return
    */
  def customrmapPartitions(line: Iterator[String]): Iterator[String] = {
    val result = new ListBuffer[String]
    while (line.hasNext) {
      result += (line.next() + "===")
    }
    result.iterator
  }

}

