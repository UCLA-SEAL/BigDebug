package org.apache.spark.examples.bigsift.benchmarks.adjacencylist

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
      val index = s.lastIndexOf(",")
      if (index == -1) false
      else true
    }).flatMap(s => {
      val listOfEdges: MutableList[(String, String)] = MutableList()
      val index = s.lastIndexOf(",")
      val outEdge = s.substring(0, index)
      val inEdge = s.substring(index + 1)
      val outList = "from{" + outEdge + "}:to{}"
      var inList = "from{}:to{" + inEdge + "}"
      val out = Tuple2(outEdge, inList)
      val in = Tuple2(inEdge, outList)
      listOfEdges += out
      listOfEdges += in
      listOfEdges
    })
      .groupByKey()
      .map(pair => {
        var fromList: mutable.MutableList[String] = mutable.MutableList()
        var toList: mutable.MutableList[String] = mutable.MutableList()
        var fromLine = new String()
        var toLine = new String()
        var vertex = new String()
        //       var itr:util.Iterator[String] = null
        //        try {
        val itr = pair._2.toIterator
        //       }catch{
        //         case e:Exception =>
        //           println("**************************")
        //       }
        while (itr.hasNext) {
          breakable {
            val str = itr.next()
            val strLength = str.length
            val index = str.indexOf(":")
            if (index == -1) {
              println("Wrong input: " + str)
              break
            }
            fromLine = AdjacencyList.extractFrom(str)
            toLine = AdjacencyList.extractTo(str)
            if (!fromLine.isEmpty) {
              val itr2 = new StringTokenizer(fromLine, ",")
              while (itr2.hasMoreTokens) {
                vertex = new String(itr2.nextToken())
                if (!fromList.contains(vertex)) {
                  fromList += vertex
                }
              }
            }
            if (!toLine.isEmpty) {
              val itr2 = new StringTokenizer(toLine, ",")
              while (itr2.hasMoreTokens) {
                vertex = new String(itr2.nextToken())
                if (!toList.contains(vertex)) {
                  toList += vertex
                }
              }
            }
          }
        }
        fromList = fromList.sortWith((a, b) => if (a < b) true else false)
        toList = toList.sortWith((a, b) => if (a < b) true else false)
        var fromList_str = new String("")
        var toList_str = new String("")
        for (r <- 0 until fromList.size) {
          if (fromList_str.equals("")) fromList_str = fromList(r)
          else fromList_str = fromList_str + "," + fromList(r)
        }
        for (r <- 0 until toList.size) {
          if (toList_str.equals("")) toList_str = toList(r)
          else toList_str = toList_str + "," + toList(r)
        }
        val outValue = new String("from{" + fromList_str + "}:to{" + toList_str + "}")
        (pair._1, outValue)
      })

    val out = wordDoc.collect()
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")

    for (o <- out) {
      //  println(o)
      if (AdjacencyList.failure(o)) returnValue = true
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
      val index = s.lastIndexOf(",")
      if (index == -1) false
      else true
    }).flatMap(s => {
      val listOfEdges: MutableList[(String, String)] = MutableList()
      val index = s.lastIndexOf(",")
      val outEdge = s.substring(0, index)
      val inEdge = s.substring(index + 1)
      val outList = "from{" + outEdge + "}:to{}"
      var inList = "from{}:to{" + inEdge + "}"
      val out = Tuple2(outEdge, inList)
      val in = Tuple2(inEdge, outList)
      listOfEdges += out
      listOfEdges += in
      listOfEdges
    })
      .groupBy(_._1)
      .map(pair => {
        var fromList: mutable.MutableList[String] = mutable.MutableList()
        var toList: mutable.MutableList[String] = mutable.MutableList()
        var fromLine = new String()
        var toLine = new String()
        var vertex = new String()
        //       var itr:util.Iterator[String] = null
        //        try {
        val itr = pair._2.toIterator
        //       }catch{
        //         case e:Exception =>
        //           println("**************************")
        //       }
        while (itr.hasNext) {
          breakable {
            val str = itr.next()._2
            val strLength = str.length
            val index = str.indexOf(":")
            if (index == -1) {
              println("Wrong input: " + str)
              break
            }
            fromLine = AdjacencyList.extractFrom(str)
            toLine = AdjacencyList.extractTo(str)
            if (!fromLine.isEmpty) {
              val itr2 = new StringTokenizer(fromLine, ",")
              while (itr2.hasMoreTokens) {
                vertex = new String(itr2.nextToken())
                if (!fromList.contains(vertex)) {
                  fromList += vertex
                }
              }
            }
            if (!toLine.isEmpty) {
              val itr2 = new StringTokenizer(toLine, ",")
              while (itr2.hasMoreTokens) {
                vertex = new String(itr2.nextToken())
                if (!toList.contains(vertex)) {
                  toList += vertex
                }
              }
            }
          }
        }
        fromList = fromList.sortWith((a, b) => if (a < b) true else false)
        toList = toList.sortWith((a, b) => if (a < b) true else false)
        var fromList_str = new String("")
        var toList_str = new String("")
        for (r <- 0 until fromList.size) {
          if (fromList_str.equals("")) fromList_str = fromList(r)
          else fromList_str = fromList_str + "," + fromList(r)
        }
        for (r <- 0 until toList.size) {
          if (toList_str.equals("")) toList_str = toList(r)
          else toList_str = toList_str + "," + toList(r)
        }
        val outValue = new String("from{" + fromList_str + "}:to{" + toList_str + "}")
        (pair._1, outValue)
      })

    val out = wordDoc
    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")
    for (o <- out) {
      //  println(o)
      if (AdjacencyList.failure(o)) returnValue = true
    }
    return returnValue
  }

}