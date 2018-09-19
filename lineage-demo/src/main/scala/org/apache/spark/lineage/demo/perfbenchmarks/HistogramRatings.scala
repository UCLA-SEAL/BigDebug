package org.apache.spark.lineage.demo.perfbenchmarks

import java.util.StringTokenizer

import org.apache.spark.SparkConf
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.demo.LineageBaseApp
import org.apache.spark.lineage.rdd.Lineage

/**
 * Modified by Jason (jteoh) on 9/19/18. Also, not sure what changes (if any) were introduced by
 * Katherine.
 * Modified by Katherine on 8/10/18
 * Created by malig on 11/30/16.
 */
object HistogramRatings extends LineageBaseApp(
                                              threadNum = Some(6), // jteoh retained from original
                                              lineageEnabled = true,
                                              sparkLogsEnabled = false,
                                              sparkEventLogsEnabled = true,
                                              igniteLineageCloseDelay = 5 * 1000
                                             ) {
  var logFile: String = _
  val WITH_ARTIFICIAL_DELAY  = false // TODO there is no artificial delay introduced here yet.
  override def initConf(args: Array[String], defaultConf: SparkConf): SparkConf = {
    // jteoh: only conf-specific configuration is this one, which might not be required for usual
    // execution.
    defaultConf.set("spark.executor.memory", "2g")
    // 2106 lines, 98MB
    logFile = args.headOption.getOrElse("/Users/jteoh/Code/BigSummary-Experiments/experiments/MoviesAnalysis/data/file1s.data")
    defaultConf.setAppName(s"${appName}-lineage:${lineageEnabled}-${logFile}")
  }
  
override def run(lc: LineageContext, args: Array[String]): Unit = {
  //set up logging
  //      val lm: LogManager = LogManager.getLogManager
  //      val logger: Logger = Logger.getLogger(getClass.getName)
  //      val fh: FileHandler = new FileHandler("myLog")
  //      fh.setFormatter(new SimpleFormatter)
  //      lm.addLogger(logger)
  //      logger.setLevel(Level.INFO)
  //      logger.addHandler(fh)
  //set up spark configuration
  
  //  val sparkConf = new SparkConf()
  //
  //  var logFile = ""
  //  var local = 500
  //  if (args.length < 2) {
  //    sparkConf.setMaster("local[6]")
  //    sparkConf.setAppName("Histogram Movies").set("spark.executor.memory", "2g")
  //    logFile = "/home/ali/work/temp/git/bigsift/src/benchmarks/histogrammovies/data/file1s.data"
  //  } else {
  //    logFile = args(0)
  //    local = args(1).toInt
  //  }
  
  //set up lineage
  //      var lineage = true
  //      lineage = true
  
  //val ctx = new SparkContext(sparkConf)
  
  //start recording time for lineage
  /** ************************
   * Time Logging
   * *************************/
  //      val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
  //      val jobStartTime = System.nanoTime()
  //      logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
  /** ************************
   * Time Logging
   * *************************/
  
  val lines = lc.textFile(logFile, 1)
  
  //Compute once first to compare to the groundTruth to trace the lineage
  val ratings = lines.flatMap(s => {
    var ratingMap : Map[Int, Int] =  Map()
    var rating: Int = 0
    var reviewIndex: Int = 0
    var movieIndex: Int = 0
    var reviews: String = new String
    var tok: String = new String
    var ratingStr: String = new String
    var raterStr: String = new String
    var movieStr:String = new String
    movieIndex = s.indexOf(":")
    if (movieIndex > 0) {
      reviews = s.substring(movieIndex + 1)
      movieStr = s.substring(0,movieIndex)
      val token: StringTokenizer = new StringTokenizer(reviews, ",")
      while (token.hasMoreTokens) {
        tok = token.nextToken
        reviewIndex = tok.indexOf("_")
        raterStr = tok.substring(0, reviewIndex)
        ratingStr = tok.substring(reviewIndex + 1)
        rating = ratingStr.toInt
        var rater = raterStr.toLong
        if (rating == 1 || rating == 2) {
          if (rater % 13 != 0) {
            val old_rat = ratingMap.getOrElse(rating, 0)
            ratingMap = ratingMap updated(rating, old_rat+1)
          }
        }
        else {
          //              if(movieStr.equals("1995670000") && raterStr.equals("2256305") && rating == 5) {
          //                val old_rat = ratingMap.getOrElse(rating, 0)
          //                ratingMap =       ratingMap updated(rating, old_rat + Int.MinValue)
          //              }else {
          val old_rat = ratingMap.getOrElse(rating, 0)
          ratingMap = ratingMap updated(rating, old_rat+1)
          //              }
        }
      }
    }
    ratingMap.toIterable
  })
  val counts  = ratings.reduceByKey(_+_)
  val output = Lineage.measureTimeWithCallback(counts.collect(),
                                               x => println(s"Collect time: $x ms"))
  
  println("Counts: " + output.length)
  output.foreach(println)
  /** ************************
   * Time Logging
   * *************************/
  //      println(">>>>>>>>>>>>>  Original Job Done  <<<<<<<<<<<<<<<")
  //      val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
  //      val jobEndTime = System.nanoTime()
  //      logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
  //      logger.log(Level.INFO, "JOb span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")
  
  /** ************************
   * Time Logging
   * *************************/
  
  
  
  /** ************************
   * Time Logging
   * *************************/
  //      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
  //      val DeltaDebuggingStartTime = System.nanoTime()
  //      logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
  /** ************************
   * Time Logging
   * *************************/
  
  
  //      val delta_debug = new DDNonExhaustive[String]
  //      delta_debug.setMoveToLocalThreshold(local);
  //      val returnedRDD = delta_debug.ddgen(lines, new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)
  
  /** ************************
   * Time Logging
   * *************************/
  //      val DeltaDebuggingEndTime = System.nanoTime()
  //      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
  //      logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
  //      logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")
  
  /** ************************
   * Time Logging
   * *************************/
  
  println("Job's DONE!")
  }
  
  def failure(record:Int): Boolean ={
    record< 0
  }
  
}
