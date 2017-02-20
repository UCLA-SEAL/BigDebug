package org.apache.spark.examples.bigsift.benchmarks.selfjoin

import java.util.logging._
import java.util.{Collections, Calendar, StringTokenizer}

import org.apache.spark.examples.bigsift.bigsift.{DDNonExhaustive, SequentialSplit}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.MutableList


/**
  * Created by malig on 11/30/16.
  *
  * Error is seeded for a particular edge p,q pair.
  * Test returns false if an edge p contains q in Kth or K+1 item
  * lineage return all the rows that involve p.
  * Faulty line is  :
  * 1..k-1 => entryNum01456551332,entryNum10572557743,entryNum13723283663,entryNum33874871442,entryNum48736261267,entryNum54536860027,entryNum64168585873
  *
  * k => entryNum64370042335
  *
  *
  **/

object SelfJoin {

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
      if(SelfJoin.failure(o)){
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


  def failure(record: (Any, Any)): Boolean = {
    return record._1 match {
      case _: Tuple2[_, _] =>
        var o = record._1.asInstanceOf[Tuple2[String, String]]
        if (o._1.equalsIgnoreCase("entryNum01456551332,entryNum10572557743,entryNum13723283663,entryNum33874871442,entryNum48736261267,entryNum54536860027,entryNum64168585873")) {
          val edge = o._1
          o._2.contains("entryNum64370042335")
        }
        else {
          false
        }
      case _: String =>
        var o = record.asInstanceOf[Tuple2[String, String]]
        if (o._1.equalsIgnoreCase("entryNum01456551332,entryNum10572557743,entryNum13723283663,entryNum33874871442,entryNum48736261267,entryNum54536860027,entryNum64168585873")) {
          val edge = o._1
          o._2.contains("entryNum64370042335")
        }
        else {
          false
        }
    }
  }
}
