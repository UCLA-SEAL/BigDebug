package org.apache.spark.lineage.demo.injecteddelays

import java.util.StringTokenizer

import org.apache.commons.lang3.StringUtils
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
object HistogramMoviesInjectedDataFile extends LineageBaseApp(
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
    logFile = args.headOption.getOrElse("/Users/jteoh/Code/BigSummary-Experiments/experiments/MoviesAnalysis/data" +
      "/file1s_jteoh_injected_extra_long_records.data")
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
  //val delayInjectedLines = lines.map(injectDelays)
  //Compute once first to compare to the groundTruth to trace the lineage
  val averageRating: Lineage[(Double, Int)] = lines.map { s =>
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
      movieStr = s.substring(0, movieIndex)
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
//    if(injectionMovies.contains(movieStr)) {
//      println(s"Movie $movieStr has average $avg")
//    }
    if(movieStr.equals("1995670000")) (avg , Int.MinValue) else (avg, 1)
  }
  val counts = averageRating.reduceByKey(_+_)//.filter(a=> failure(a._2))
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
  
  val injectionMovies = Set("100","574","1220","1933","1564")
  private def injectDelays(line: String): String = {
    val movie = StringUtils.substringBefore(line, ":")
    movie match {
      // each string corresponds to the line number. There are 2103 lines total.
      // unless otherwise specified, there is no significance behind the line numbers.
      case "100" =>
        // displayed average of 5.0, should be returned with an estimate of 5000->3500
        Thread.sleep(5000)
      case "574" =>
        // displayed average of 6.0, should be returned with an estimate of 3000->0ish
        Thread.sleep(3000)
      case "1220" =>
        // displayed average of 8.0, should be returned with an average of 2500->0ish
        Thread.sleep(2500)
      case "1933" =>
        // displayed average of 5.0, dominated by the "100" case. We expect removal of "100" to
        // score roughly 5000->1500.
        Thread.sleep(1500)
      case "1564" =>
        // average of 7.0, should be returned with an average of 1000->0ish
        Thread.sleep(1000)
      case _ => // noop
    }
    line
  }
}
