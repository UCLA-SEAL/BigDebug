package org.apache.spark.examples.bigsift.benchmarks.adjacencylist

import java.util.logging._

import org.apache.spark.examples.bigsift.bigsift.{SequentialSplit, DDNonExhaustive}
import org.apache.spark.{ SparkConf, SparkContext}

import java.util.{ Calendar, StringTokenizer}

import scala.collection.mutable
import scala.collection.mutable.{ MutableList}


import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

/**
  * Created by malig on 11/30/16.
  *
  * Error is seeded for a particular edge p,q pair.
  * Test returns false if an edge p contains q in from{} or to{} list
  * lineage return all the rows that involve p.
  * Faulty line is  : VertexID00048465278_31,VertexID00044383444_31
  *
  *
  **/

object AdjacencyList {

  private val division = 0.5f
  private val exhaustive = 1

  def mapFunc(str: String): (Float, Int) = {
    val token = new StringTokenizer(str)
    val bin = token.nextToken().toFloat
    val value = token.nextToken().toInt
    return (bin, value)
  }

  def main(args: Array[String]) {
    try {
      //set up logging
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)
      lm.addLogger(logger)
      logger.setLevel(Level.INFO)
      logger.addHandler(fh)
      //set up spark configuration
      val sparkConf = new SparkConf()

      var logFile = ""
      var local = 500
      if (args.length < 2) {
        sparkConf.setMaster("local[6]")
        sparkConf.setAppName("Inverted Index").set("spark.executor.memory", "2g")
        logFile = "/home/ali/work/temp/git/bigsift/src/benchmarks/histogrammovies/data/file1s.data"
      } else {

        logFile = args(0)
        local = args(1).toInt

      }
      //set up lineage
      var lineage = true
      lineage = true

      val ctx = new SparkContext(sparkConf)

      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)

      //start recording time for lineage
      /** ************************
        * Time Logging
        * *************************/
      val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val jobStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
      /** ************************
        * Time Logging
        * *************************/

      val lines = lc.textFile(logFile, 1)

      //Compute once first to compare to the groundTruth to trace the lineage
      val resultEdges = lines.filter(s => {
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

      val output_result = resultEdges.collectWithId()

      /** ************************
        * Time Logging
        * *************************/
      println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
      val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val jobEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *************************/


      lc.setCaptureLineage(false)
      Thread.sleep(1000)


      var listl = List[Long]()
      for (o <- output_result) {
      if(AdjacencyList.failure(o)){
        listl = o._2 :: listl
        }
      }
      /** ************************
        * Time Logging
        * *************************/
      val lineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val lineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + lineageStartTimestamp)
      /** ************************
        * Time Logging
        * *************************/

      var linRdd = resultEdges.getLineage()
      linRdd.collect
      linRdd = linRdd.filter { l => listl.contains(l) }
      linRdd = linRdd.goBackAll()
      val showMeRdd = linRdd.show(false).toRDD

      /** ************************
        * Time Logging
        * *************************/
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val lineageEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + lineageEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (lineageEndTime - lineageStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *************************/


      /** ************************
        * Time Logging
        * *************************/
      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
      /** ************************
        * Time Logging
        * *************************/


      val delta_debug = new DDNonExhaustive[String]
      delta_debug.setMoveToLocalThreshold(local);
      val returnedRDD = delta_debug.ddgen(showMeRdd, new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)

      /** ************************
        * Time Logging
        * *************************/
      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")

      /** ************************
        * Time Logging
        * *************************/

      println("Job's DONE!")
      ctx.stop()
    }
  }

  def extractFrom(str: String): String = {
    var fromLine = ""
    val in = str.indexOf(":")
    if (in > 6) {
      fromLine = str.substring(5, in - 1)
    }
    fromLine
  }

  def extractTo(str: String): String = {
    var toLine = ""
    val l = str.length
    val in = str.indexOf(":")
    if (in + 5 < l) {
      toLine = str.substring(in + 4, l - 1)
    }
    toLine
  }

  def failure(record: (Any, Any)): Boolean = {
    return record._1 match {
      case _: Tuple2[_, _] =>
        var o = record._1.asInstanceOf[Tuple2[String, String]]
        if (o._1.equalsIgnoreCase("VertexID00048465278_31")) {
          val edge = o._1
          var toLine = AdjacencyList.extractTo(o._2)
          var fromLine = AdjacencyList.extractTo(o._2)
          toLine.contains("VertexID00044383444_31") || fromLine.contains("VertexID00044383444_31")
        }
        else {
          false
        }
      case _: String =>
        var o = record.asInstanceOf[Tuple2[String, String]]
        if (o._1.equalsIgnoreCase("VertexID00048465278_31")) {
          val edge = o._1
          var toLine = AdjacencyList.extractTo(o._2)
          var fromLine = AdjacencyList.extractTo(o._2)
          toLine.contains("VertexID00044383444_31") || fromLine.contains("VertexID00044383444_31")
        }
        else {
          false
        }
    }
  }
}
