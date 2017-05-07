package org.apache.spark.examples.bigsift.benchmarks.invertedindex

import java.io.Serializable
import java.util.logging.{Logger, FileHandler, LogManager}

import org.apache.spark.examples.bigsift.bigsift.interfaces.Testing
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList
import org.apache.spark.SparkContext._

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
    val wordDoc = inputRDD.flatMap(s => {
      val wordDocList: MutableList[(String, String)] = MutableList()
      val colonIndex = s.lastIndexOf("^")
      val docName = s.substring(0, colonIndex).trim()
      val content = s.substring(colonIndex + 1)
      val wordList = content.trim.split(" ")
      for (w <- wordList) {
        wordDocList += Tuple2(w, docName)
      }
      wordDocList.toList
    })
      .filter(r => InvertedIndex.filterSym(r._1))
      .map{
      p =>
        val docSet = scala.collection.mutable.Set[String]()
        docSet += p._2
        (p._1 , (p._1,docSet))
    }.reduceByKey{
      (s1,s2) =>
        val s = s1._2.union(s2._2)
        (s1._1, s)
    }.filter(s => InvertedIndex.failure((s._1,s._2._2)))
    val out = wordDoc.collect()
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
    val wordDoc = inputRDD.flatMap(s => {
      val wordDocList: MutableList[(String, String)] = MutableList()
      val colonIndex = s.lastIndexOf("^")
      val docName = s.substring(0, colonIndex).trim()
      val content = s.substring(colonIndex + 1)
      val wordList = content.trim.split(" ")
      for (w <- wordList) {
        wordDocList += Tuple2(w, docName)
      }
      wordDocList.toList
    })
      .filter(r => InvertedIndex.filterSym(r._1))
      .groupBy(_._1)
      .map(pair => {
      val docSet = scala.collection.mutable.Set[String]()
      var value = new String("")
      val itr = pair._2.toIterator
      var word = pair._1
      while (itr.hasNext) {
        val doc  = itr.next()._2
        docSet += doc
      }
      (word, docSet)
    }).filter(s => InvertedIndex.failure(s))

    val out = wordDoc
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")
    for (o <- out) {
      returnValue = true
    }
    return returnValue
  }

}