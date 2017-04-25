package org.apache.spark.examples.bigsift.benchmarks.ratersfrequency

import java.util.logging._
import java.util.{Calendar, StringTokenizer}

import org.apache.spark.examples.bigsift.benchmarks.histogramratings.HistogramRatings
import org.apache.spark.examples.bigsift.bigsift.{DDNonExhaustive, SequentialSplit}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by malig on 11/30/16.
  */

object HistogramRatersOverlap {

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
      var applySJF = 0
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
            list += addFault(rater, movieStr)

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
      if (applySJF == 1) {
        while (list.size != 0) {
          if (list.size > 1) {
            logger.log(Level.INFO, s""" Overlap : More than two faults  """)
            val e1 = list(0)
            val e2 = list(1)
            var linRdd = ratings.getLineage()
            linRdd.collect
            val mappedRDD1 = linRdd.filter { l => e1._1 == l}.goBackAll().show(false).toRDD
            linRdd = ratings.getLineage()
            linRdd.collect
            val mappedRDD2 = linRdd.filter { l => e2._1 == l}.goBackAll().show(false).toRDD
            val mappedRDD = mappedRDD1.intersection(mappedRDD2)
            val fault = runDD(mappedRDD, logger, local, lm, fh)
            if (!fault) {
              logger.log(Level.INFO, s""" Overlap : Contains fault  """)
              runDD(mappedRDD1, logger, local, lm, fh)
              runDD(mappedRDD2, logger, local, lm, fh)
              list = list.drop(2)
            } else {
              logger.log(Level.INFO, s""" Overlap : does not contain faults  """)
              runDD(mappedRDD1.subtract(mappedRDD), logger, local, lm, fh)
              runDD(mappedRDD2.subtract(mappedRDD), logger, local, lm, fh)
              list = list.drop(2)
            }
          } else {
            logger.log(Level.INFO, s""" Overlap : Only one fault """)
            val e1 = list(0)
            list = list.drop(1)
            val linRdd = ratings.getLineage()
            linRdd.collect
            val mappedRDD = linRdd.filter { l => e1._1 == l}.goBackAll().show(false).toRDD
            runDD(mappedRDD, logger, local, lm, fh)
          }
        }
      }else{
        logger.log(Level.INFO, s""" Overlap : disabled""")
        while (list.size != 0) {
          val e1 = list(0)
          list = list.drop(1)
          val linRdd = ratings.getLineage()
          linRdd.collect
          val mappedRDD = linRdd.filter { l => e1._1 == l}.goBackAll().show(false).toRDD
          runDD(mappedRDD, logger, local, lm, fh)
        }
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


  def runDD(maprdd : RDD[String] , logger: Logger , local:Int ,  lm: LogManager, fh: FileHandler): Boolean ={

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
    val foundfault = delta_debug.ddgen(maprdd, new TestSJF(), new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)
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
    foundfault
  }
  def addFault(rater:String ,  movieid: String) ={
    if(movieid.equals("1995670000") && rater.equals("53679")){
      (rater , -99999)
    }else if(movieid.equals("1995670000") && rater.equals("2296924")){
      (rater , -99999)
    }else if(movieid.equals("1995670000") && rater.equals("589967")){
      (rater , -99999)
    }else if(movieid.equals("1995670000") && rater.equals("1527113")){
      (rater , -99999)
    }else if(movieid.equals("1995670000") && rater.equals("1181231")){
      (rater , -99999)
    }else if(movieid.equals("39910") && rater.equals("2443370")){
      (rater , -99999)
    }else if(movieid.equals("39910") && rater.equals("1140255")){
      (rater , -99999)
//    }else if(movieid.equals("9977350000") && rater.equals("2464728")){
//      (rater , -99999)
//    }else if(movieid.equals("9977350000") && rater.equals("803605")){
//      (rater , -99999)
//    }else if(movieid.equals("9977350000") && rater.equals("1722054")){
//      (rater , -99999)
    }else{
      (rater , 1)
    }
  }
  def failure(record:Int): Boolean ={
        record< 0
  }
}
