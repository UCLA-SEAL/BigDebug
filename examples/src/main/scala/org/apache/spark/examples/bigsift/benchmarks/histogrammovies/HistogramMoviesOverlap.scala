package org.apache.spark.examples.bigsift.benchmarks.histogrammovies

import java.util.{StringTokenizer, Calendar}
import java.util.logging._

import org.apache.spark.examples.bigsift.benchmarks.histogramratings.HistogramRatings
import org.apache.spark.examples.bigsift.bigsift.{SequentialSplit, DDNonExhaustive}
import org.apache.spark.lineage.LineageContext

import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
  * Created by malig on 11/30/16.
  */

object HistogramMoviesOverlap {

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
      var jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      var jobStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
      /** ************************
        * Time Logging
        * *******************************************************************************************************************************************************************************/

      val lines = lc.textFile(logFile, 1)

      //Compute once first to compare to the groundTruth to trace the lineage
      val averageRating = lines.map { s =>
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
            ratingStr = tok.substring(reviewIndex + 1)
            rating = java.lang.Integer.parseInt(ratingStr)
              sumRatings += rating
              totalReviews += 1
          }
          avgReview = sumRatings.toFloat / totalReviews.toFloat

        }
        val avg = Math.floor(avgReview * 2.toDouble)
        if(movieStr.equals("1995670000")) (avg , Int.MinValue) else (avg, 1)
      }
      val counts = averageRating.groupByKey()
        .map(pair => {
        var total = 0
        for (num <- pair._2) {
          total += num
        }
        (pair._1/2, total)
      }).filter(a=> HistogramMovies.failure(a._2))
      val output1 = counts.collectWithId()
      println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
      println(">>>>>>>>>>>>>  Starting First Trace  <<<<<<<<<<<<<<<")
      /** ******************************************************************************************************************************************************************************
        * Time Logging
        * *************************/

      var jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      var jobEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *******************************************************************************************************************************************************************************/
      lc.setCaptureLineage(false)
      Thread.sleep(1000)
      var list = List[Long]()
      for (o <- output1) {
        list = o._2 :: list

      }
      /** ******************************************************************************************************************************************************************************
        * Time Logging
        * *************************/
      var lineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      var lineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + lineageStartTimestamp)
      /** ************************
        * Time Logging
        * *******************************************************************************************************************************************************************************/

      var linRdd = counts.getLineage()
      linRdd.collect
      linRdd = linRdd.filter { l => list.contains(l) }
      linRdd = linRdd.goBackAll()
      val showMeRdd = linRdd.show(false).toRDD

      println(">>>>>>>>>>>>>  1 lineage count : "+showMeRdd.count()+"  <<<<<<<<<<<<<<<")
      /** ******************************************************************************************************************************************************************************
        * Time Logging
        * *************************/
      var lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      var lineageEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + lineageEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (lineageEndTime - lineageStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *************************/
      /** ************************
        * Time Logging
        * *************************/
       jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
       jobStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
      /** ************************
        * Time Logging
        * *******************************************************************************************************************************************************************************/
      lc.setCaptureLineage(true)

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
      val output2 = ratings.collectWithId()

      println(">>>>>>>>>>>>>  Second Job Done  <<<<<<<<<<<<<<<")
      println(">>>>>>>>>>>>>  Starting Second Trace  <<<<<<<<<<<<<<<")

      /** ******************************************************************************************************************************************************************************
        * Time Logging
        * *************************/

       jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
       jobEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *******************************************************************************************************************************************************************************/


      lc.setCaptureLineage(false)
      Thread.sleep(1000)


     list = List[Long]()
      for (o <- output2) {
        list = o._2 :: list

      }

      /** ******************************************************************************************************************************************************************************
        * Time Logging
        * *************************/
       lineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
       lineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + lineageStartTimestamp)
      /** ************************
        * Time Logging
        * *******************************************************************************************************************************************************************************/

      var linRdd2 = ratings.getLineage()
      linRdd2.collect
      linRdd2 = linRdd2.filter { l => list.contains(l) }
      linRdd2 = linRdd2.goBackAll()
      val showMeRdd2 = linRdd2.show(false).toRDD

      /** ******************************************************************************************************************************************************************************
        * Time Logging
        * *************************/
       lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
       lineageEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + lineageEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (lineageEndTime - lineageStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *******************************************************************************************************************************************************************************/


      /***Applying the intersect*/

      println(">>>>>>>>>>>>>  2 lineage count : "+showMeRdd2.count()+"  <<<<<<<<<<<<<<<")
      val intersect = showMeRdd.intersection(showMeRdd2)



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
      val returnedRDD = delta_debug.ddgen(intersect , new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)


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
