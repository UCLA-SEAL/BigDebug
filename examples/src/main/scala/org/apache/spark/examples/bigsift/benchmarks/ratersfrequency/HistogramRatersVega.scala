package org.apache.spark.examples.bigsift.benchmarks.ratersfrequency

import java.util.logging._
import java.util.{Calendar, StringTokenizer}

import org.apache.spark.examples.bigsift.benchmarks.histogramratings.HistogramRatings
import org.apache.spark.examples.bigsift.bigsift.{DDNonExhaustiveVega, DDNonExhaustive, SequentialSplit}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by malig on 11/30/16.
  */

object HistogramRatersVega {

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

      val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
       val jobEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *******************************************************************************************************************************************************************************/


      lc.setCaptureLineage(false)
      Thread.sleep(1000)


    var list = List[Long]()
      for (o <- output2) {
        list = o._2 :: list

      }

      /** ******************************************************************************************************************************************************************************
        * Time Logging
        * *************************/
      val lineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
       val lineageStartTime = System.nanoTime()
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
       val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
       val lineageEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + lineageEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (lineageEndTime - lineageStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *******************************************************************************************************************************************************************************/


      /** ******************************************************************************************************************************************************************************
        * Time Logging
        * *************************/
      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
      /** ************************
        * Time Logging
        * ****************************************************************************************************************************************************************************/


       val delta_debug = new DDNonExhaustiveVega[String , (String, Int)]
      delta_debug.setMoveToLocalThreshold(local);
      val test = new TestVega
      test.setInverse(inverse)
      val returnedRDD = delta_debug.ddgen(showMeRdd2 , test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)


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
  def inverse(rdd1: Array[(String,Int)]  , rdd2:Array[(String,Int)]): Array[(String, Int)] ={
    val map1 = scala.collection.mutable.HashMap[String, Int]()
    val map2 = scala.collection.mutable.HashMap[String, Int]()
    rdd1.map(x => map1(x._1)= x._2)
    rdd2.map(x => map2(x._1)= x._2)
    val joined = map1.map(x=>  (x._1,x._2 - map2.getOrElse(x._1,0)))
    joined.filter(v => v._2 !=0).toArray
  }

}
