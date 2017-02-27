package org.apache.spark.examples.bigsift.benchmarks.ratersfrequency

import java.util.logging._
import java.util.{Calendar, StringTokenizer}

import org.apache.spark.examples.bigsift.benchmarks.histogramratings.HistogramRatings
import org.apache.spark.examples.bigsift.bigsift.{DDNonExhaustive, SequentialSplit}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by malig on 11/30/16.
  */

object HistogramRatersSJF {

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
      var applySJF =0
      if (args.length < 2) {
        sparkConf.setMaster("local[6]")
        sparkConf.setAppName("Inverted Index").set("spark.executor.memory", "2g")
        logFile = "/home/ali/work/temp/git/bigsift/src/benchmarks/histogrammovies/data/file1s.data"
      } else {
        applySJF = args(2).toInt
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
      var jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      var jobStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
      /** ************************
        * Time Logging
        * *******************************************************************************************************************************************************************************/

      val ratings  =  lc.textFile(logFile, 1).flatMap{ s =>
        val list: mutable.MutableList[(String, Int)] = mutable.MutableList()
        var rating: Int = 0
        var movieIndex: Int = 0
        var reviewIndex: Int = 0
        var totalReviews = 0
        var sumRatings = 0
        var avgReview = 0.0f
        var absReview: Float = 0.0f
        var fraction: Float = 0.0f
        var outValue = 0.0f
        var reviews = new String()
        //var line = new String()
        var tok = new String()
        var ratingStr = new String()
        var fault = false
        var movieStr = new String
        movieIndex = s.indexOf(":")
        if (movieIndex > 0) {
          reviews = s.substring(movieIndex + 1)
          movieStr = s.substring(0,movieIndex)
          val token = new StringTokenizer(reviews, ",")
          while (token.hasMoreTokens()) {
            tok = token.nextToken()
            reviewIndex = tok.indexOf("_")
            val rater = tok.substring(0,reviewIndex).trim()
            ratingStr = tok.substring(reviewIndex + 1)
            rating = java.lang.Integer.parseInt(ratingStr)
              list += HistogramRatersOverlap.addFault(rater, movieStr)
          }

        }
        list.toList
      }.groupByKey()
        .map(pair => {
        var total = 0
        for (num <- pair._2) {
          total += num
        }
        (pair._1, total)
      }).filter(s => HistogramRatings.failure(s._2))
      val output = ratings.collectWithId()

      println(">>>>>>>>>>>>>  Second Job Done  <<<<<<<<<<<<<<<")
      println(">>>>>>>>>>>>>  Starting Second Trace  <<<<<<<<<<<<<<<")

      /** ******************************************************************************************************************************************************************************
        * Time Logging
        * *************************/

      val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
       val jobEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *******************************************************************************************************************************************************************************/


      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      /** ************************
        * Time Logging
        * *************************/
      val lineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val lineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + lineageStartTimestamp)
      /** ************************
        * Time Logging
        * *************************/




      //list of bad inputs
      var list = List[Tuple2[Long, Long]]() // linID and size
      for (o <- output) {
        var linRdd = ratings.getLineage()
        linRdd.collect
        var size = linRdd.filter { l => o._2 == l }.goBackAll().count()
        list = Tuple2(o._2, size) :: list
      }

      if (applySJF == 1) {
        list = list.sortWith(_._2 < _._2)
      }


      /** ************************
        * Time Logging
        * *************************/
      println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
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
      val SJFStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val SJFStartTime = System.nanoTime()
      logger.log(Level.INFO, "SJF (unadjusted) time starts at " + SJFStartTimestamp)

      /** ************************
        * Time Logging
        * *************************/


      for (e <- list) {
        var linRdd = ratings.getLineage()
        linRdd.collect
        val mappedRDD = linRdd.filter { l => e._1 == l }.goBackAll().show(false).toRDD
        logger.log(Level.INFO, s"""Debugging Lineage [id , size] : $e""")


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
        delta_debug.setMoveToLocalThreshold(local)
        val returnedRDD = delta_debug.ddgen(mappedRDD, new TestSJF(), new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)
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

      }


      /** ************************
        * Time Logging
        * *************************/
      val SJFEndTime = System.nanoTime()
      val SJFEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "SJF (unadjusted) + L  ends at " + SJFEndTimestamp)
      logger.log(Level.INFO, "SJF (unadjusted) + L takes " + (SJFEndTime - SJFStartTime) / 1000 + " milliseconds")

      /** ************************
        * Time Logging
        * *************************/




      println("Job's DONE!")
      ctx.stop()
    }
  }
  def failure(record:Int): Boolean ={
        record< 0
  }
}
