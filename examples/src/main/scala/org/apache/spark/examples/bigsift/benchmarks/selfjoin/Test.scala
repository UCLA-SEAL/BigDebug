package org.apache.spark.examples.bigsift.benchmarks.selfjoin

import java.io.Serializable
import java.util.StringTokenizer
import java.util.logging.{FileHandler, LogManager, Logger}

import org.apache.spark.SparkContext._
import org.apache.spark.examples.bigsift.bigsift.interfaces.Testing
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.MutableList
import scala.util.control.Breaks._

/**
  * Created by malig on 11/30/16.
  */

class Test extends Testing[String] with Serializable {
  var num = 0;

  def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass which returns false
    var returnValue = false
    val wordDoc = inputRDD.filter(s => {
      var index = 0
      index = s.lastIndexOf(",")
      if (index == -1) {
        false
      }
      else true
    })
      .map(s => {
        var kMinusOne = new String
        var kthItem  = new String
        var index: Int = 0
        index = s.lastIndexOf(",")
        if (index == -1) {
          //This line should never be printed out thanks to the filter operation above
          System.out.println("MapToPair: Input File in Wrong Format When Processing " + s)
        }
        kMinusOne = s.substring(0, index)
        kthItem = s.substring(index + 1)
        //println(kthItem.getClass.getSimpleName)
        (kMinusOne, kthItem)
        //elem
      })
      .groupByKey()
      //.reduceByKey(_+ ";" + _)
      .map(stringList1 => {
      val kthItemList: MutableList[String] = MutableList()
      for (s <- stringList1._2) {
        if (!kthItemList.contains(s)) {
          kthItemList += s
        }
      }
      val b = kthItemList.sortWith(_<_)

      (stringList1._1, b.toList)
    })
      .filter(pair => {
        if (pair._2.size < 2) false
        else true
      })
      .flatMap(stringList => {
        val output: MutableList[(String, String)] = MutableList()
        val kthItemList: List[String] = stringList._2.toList
        for (i <- 0 until (kthItemList.size - 1)) {
          for (j <- (i + 1) until kthItemList.size) {
            val outVal = kthItemList(i) + "," + kthItemList(j)
            output += Tuple2(stringList._1, outVal)
          }
        }
        output.toList
      })

    val out = wordDoc.collect()
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")

    for (o <- out) {
      //  println(o)
      if (SelfJoin.failure(o)) returnValue = true
    }
    return returnValue
  }

  def usrTest(inputRDD: Array[String], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass which returns false
    var returnValue = false
    val wordDoc = inputRDD.filter(s => {
      var index = 0
      index = s.lastIndexOf(",")
      if (index == -1) {
        false
      }
      else true
    })
      .map(s => {
        var kMinusOne = new String
        var kthItem  = new String
        var index: Int = 0
        index = s.lastIndexOf(",")
        if (index == -1) {
          //This line should never be printed out thanks to the filter operation above
          System.out.println("MapToPair: Input File in Wrong Format When Processing " + s)
        }
        kMinusOne = s.substring(0, index)
        kthItem = s.substring(index + 1)
        //println(kthItem.getClass.getSimpleName)
        (kMinusOne, kthItem)
        //elem
      })
      .groupBy(_._1)
      //.reduceByKey(_+ ";" + _)
      .map(stringList1 => {
      val kthItemList: MutableList[String] = MutableList()
      for (s <- stringList1._2) {
        if (!kthItemList.contains(s._2)) {
          kthItemList += s._2
        }
      }
      val b = kthItemList.sortWith(_<_)

      (stringList1._1, b.toList)
    })
      .filter(pair => {
        if (pair._2.size < 2) false
        else true
      })
      .flatMap(stringList => {
        val output: MutableList[(String, String)] = MutableList()
        val kthItemList: List[String] = stringList._2.toList
        for (i <- 0 until (kthItemList.size - 1)) {
          for (j <- (i + 1) until kthItemList.size) {
            val outVal = kthItemList(i) + "," + kthItemList(j)
            output += Tuple2(stringList._1, outVal)
          }
        }
        output.toList
      })
    val out = wordDoc
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")
    for (o <- out) {
      //  println(o)
      if (SelfJoin.failure(o)) returnValue = true
    }
    return returnValue
  }

}