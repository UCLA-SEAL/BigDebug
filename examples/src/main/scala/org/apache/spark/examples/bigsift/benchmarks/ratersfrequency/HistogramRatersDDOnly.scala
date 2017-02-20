package org.apache.spark.examples.bigsift.benchmarks.ratersfrequency

import java.util.logging._
import java.util.{Calendar, StringTokenizer}

import org.apache.spark.examples.bigsift.benchmarks.histogramratings.HistogramRatings
import org.apache.spark.examples.bigsift.bigsift.{DDNonExhaustive, SequentialSplit}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection.mutable

/**
  * Created by malig on 11/30/16.
  */

object HistogramRatersDDOnly {

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
      val lines = ctx.textFile(logFile, 1)
      val ratings  =  lines.flatMap{ s =>
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
            if(movieStr.equals("1995670000") && rater.equals("53679"))
              list += Tuple2(rater, -999999)
            else
               list += Tuple2(rater, 1)
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
      val output2 = ratings.collect()
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

    var list = List[Long]()
      for (o <- output2) {
        list = o._2 :: list

      }

      /** ******************************************************************************************************************************************************************************
        * Time Logging
        * *************************/
      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
      /** ************************
        * Time Logging
        * ****************************************************************************************************************************************************************************/


       val delta_debug = new DDNonExhaustive[String]
      delta_debug.setMoveToLocalThreshold(local);
      val returnedRDD = delta_debug.ddgen(lines , new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)


      /** ******************************************************************************************************************************************************************************
        * Time Logging
        * *************************/
      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")

      /** ************************
        * Time Logging
        * **************************/



      println("Job's DONE!")
      ctx.stop()
    }
  }
  def failure(record:Int): Boolean ={
        record< 0
  }
}
