package org.apache.spark.examples.bigsift.benchmarks.weather

import java.util.{StringTokenizer, Calendar}
import java.util.logging._
import org.apache.spark.examples.bigsift.bigsift.{SequentialSplit, DDNonExhaustive}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by ali on 2/25/17.
 */
object WeatherAnalysisOverlap {

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

        logFile = args(0)
        local = args(1).toInt
        applySJF = args(2).toInt

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
      val split = lines.flatMap{s =>
        val tokens = s.split(",")
        // finds the state for a zipcode
        var state = zipToState(tokens(0))
        var date = tokens(1)
        // gets snow value and converts it into millimeter
        val snow = convert_to_mm(tokens(2))
        //gets year
        val year = date.substring(date.lastIndexOf("/"))
        // gets month / date
        val monthdate= date.substring(0,date.lastIndexOf("/")-1)
        List[((String , String) , Float)](
          ((state , monthdate) , snow) ,
          ((state , year)  , snow)
        ).iterator
      }
      val deltaSnow = split.groupByKey().map{ s  =>
        val delta =  s._2.max - s._2.min
        (s._1 , delta)
      }.filter(s => WeatherAnalysis.failure(s._2))
      val output = deltaSnow.collectWithId()

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
        var linRdd = deltaSnow.getLineage()
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
            var linRdd = deltaSnow.getLineage()
            linRdd.collect
            val mappedRDD1 = linRdd.filter { l => e1._1 == l}.goBackAll().show(false).toRDD
            linRdd = deltaSnow.getLineage()
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
            val linRdd = deltaSnow.getLineage()
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
          val linRdd = deltaSnow.getLineage()
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
    val foundfault = delta_debug.ddgen(maprdd, new Test(), new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)
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

  def convert_to_mm(s: String): Float = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit match {
      case "mm" => return v
      case _ => return v * 304.8f
    }
  }
  def failure(record:Float): Boolean ={
    record > 6000f
  }
  def zipToState(str : String):String = {
    return (str.toInt % 50).toString
  }


}
