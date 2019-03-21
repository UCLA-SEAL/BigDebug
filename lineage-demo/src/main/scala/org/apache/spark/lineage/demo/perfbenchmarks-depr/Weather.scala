package org.apache.spark.lineage.demo.perfbenchmarks

import org.apache.spark.SparkConf
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.demo.LineageBaseApp
import org.apache.spark.lineage.rdd.Lineage

/** Adapted from Katherine's benchmark repository, which is in turn adapted from the BigSift
 * Benchmarks.
 */
object Weather extends LineageBaseApp(
                                      threadNum = Some(6), // jteoh retained from original
                                      lineageEnabled = true,
                                      sparkLogsEnabled = false,
                                      sparkEventLogsEnabled = true,
                                      igniteLineageCloseDelay = 10000
                                     ){
  var logFile: String = _
  val WITH_ARTIFICIAL_DELAY  = false
  
  override def initConf(args: Array[String], defaultConf: SparkConf): SparkConf = {
    // jteoh: only conf-specific configuration is this one, which might not be required for usual
    // execution.
    // defaultConf.set("spark.executor.memory", "2g")
    logFile = args.headOption.getOrElse("/Users/jteoh/Code/BigSummary-Experiments/experiments/WeatherAnalysis/data/part-00000")
    defaultConf.setAppName(s"${appName}-${logFile}")
  }
  
  override def run(lc: LineageContext, args: Array[String]): Unit = {
    
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
    val split: Lineage[((String, String), Float)] = lines.flatMap{ s =>
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
        ((state , year) , snow)
      ).iterator
    }
    val deltaSnow: Lineage[((String, String), Float)] = split.groupByKey().map{ s  =>
      val delta =  s._2.max - s._2.min
      (s._1 , delta)
    }.filter(s => addSleep(s._2))
    val output = Lineage.measureTimeWithCallback({
      deltaSnow.collect()
    }, x => println(s"Collect time: $x ms"))
    println(s"Number of outputs: ${output.length}")
    
    //outputting...
    //deltaSnow.saveAsTextFile("output.txt");
    /*
    for(each <- output) {
      println(each);
    }
    */
    
    /*
    val writer = new PrintWriter(new File("output.txt"))
    for(each <- output) {
      writer.write(each);
    }
    writer.write(output)
    writer.close()
    */
    /** ************************
     * Time Logging
     * *************************/
    //      println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
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
    //      val returnedRDD = delta_debug.ddgen(lines , new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)
    
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
  
  def convert_to_mm(s: String): Float = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit match {
      case "mm" => return v
      case _ => return v * 304.8f
    }
  }
  
  val addSleep = if(WITH_ARTIFICIAL_DELAY) {
    record:Float => {
      if (record < 500f) {
        Thread.sleep(500)
      }
      true
    }
  } else {
    // dummy filter
    record: Float => true
  }
  
  def zipToState(str : String):String = {
    return (str.toInt % 50).toString
  }

}
