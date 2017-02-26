package org.apache.spark.examples.bigsift.benchmarks.wordcount


/**
 * Created by Michael on 11/13/15.
 */

import java.io._
import java.util.StringTokenizer
import java.util.logging.{FileHandler, LogManager, Logger}

import org.apache.spark.SparkContext._
import org.apache.spark.examples.bigsift.benchmarks.sequencecount.WordCount
import org.apache.spark.examples.bigsift.bigsift.interfaces.Testing
import org.apache.spark.examples.bigsift.bigsift.interfaces.Testing
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList


class Test extends Testing[String] with Serializable {
  var num = 0;

  def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass which returns false
    var returnValue = false

    val finalRdd = inputRDD.filter(s => WordCount.filterSym(s)).flatMap(s => {
      s.split(" ").map(w  => WordCount.addFault(s , w) )
    }).reduceByKey(_+_).filter(s => WordCount.failure(s))
    val out = finalRdd.collect()
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")

    for (o <- out) {
      returnValue = true
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

    val finalRdd = inputRDD.filter(s => WordCount.filterSym(s)).flatMap(s => {
      s.split(" ").map(w  => WordCount.addFault(s , w) )
    }).groupBy(_._1).map(pair => {
      val itr = pair._2.toIterator
      var sum = 0
      while (itr.hasNext) {
        sum = sum + itr.next()._2
      }
      (pair._1, sum)
    }).filter(s => WordCount.failure(s))
    val out = finalRdd.filter(s => WordCount.failure(s))
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")
    for (o <- out) {
      returnValue = true
    }
    return returnValue
  }

}
