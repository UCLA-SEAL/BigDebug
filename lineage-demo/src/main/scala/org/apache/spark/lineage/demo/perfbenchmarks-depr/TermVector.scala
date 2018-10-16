package org.apache.spark.lineage.demo.perfbenchmarks

import org.apache.spark.SparkConf
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.demo.LineageBaseApp
import org.apache.spark.lineage.rdd.Lineage

/** Adapted from Katherine's benchmark repository, which is in turn adapted from the BigSift
 * Benchmarks. (jteoh: Not sure if any changes made by Katherine compared to original).
 */
object TermVector extends LineageBaseApp(
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
    //defaultConf.set("spark.executor.memory", "2g")
    logFile = args.headOption.getOrElse("/Users/jteoh/Documents/datasets/wiki_50GB_subset/part-00000")
    defaultConf.setAppName(s"${appName}-${logFile}")
  }
  
  override def run(lc: LineageContext, args: Array[String]): Unit = {
    //start recording time for lineage
    /**************************
        Time Logging
     **************************/
    //val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //val jobStartTime = System.nanoTime()
    //logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
    /**************************
        Time Logging
     **************************/
  
    val lines = lc.textFile(logFile, 1)
    val wordDoc: Lineage[(String, Map[String, Int])] =
      lines.map(s => {
                var wordFreqMap: Map[String, Int] = Map()
                val colonIndex = s.indexOf("^") // jteoh: changed to caret to match file format
                val docName = s.substring(0, colonIndex)
                val content = s.substring(colonIndex + 1)
                val wordList = content.trim.split(" ")
                for (w <- wordList) {
                  if(filterSym(w)){
                    if (wordFreqMap.contains(w)) {
                      // jteoh: factored out sleep operation
                      optionalSleep()
                      val newCount = wordFreqMap(w) + 1
          
                      /**** Seeding Error***/
                      //if (newCount > 10) {
                      //  wordFreqMap = wordFreqMap updated(w, 10000)
                      //}
                      /*********************/
                      //else
          
                      wordFreqMap = wordFreqMap updated(w, newCount)
                    } else {
                      wordFreqMap = wordFreqMap + (w -> 1)
                    }
                  }
                }
                // wordFreqMap = wordFreqMap.filter(p => p._2 > 1)
                (docName, wordFreqMap)
              }
          ).filter(_._2.nonEmpty)
          .reduceByKey{ (v1, v2) =>
                          var map: Map[String, Int] = Map()
                          map = v1
                          for((k,v) <- v2){
                            if(map.contains(k)){
                              val count = map(k)+ v
                              map = map updated(k, count)
                            }else{
                              map = map + (k -> v) // jteoh: fixed to k->v rather than k->1
                            }
                          }
                          map
          }//.filter(s => failure(s._2))
    val out = Lineage.measureTimeWithCallback({
      wordDoc.collect()
    }, x => println(s"Collect time: $x ms"))
    println(s"Number of outputs: ${out.length}")
  
    /**************************
        Time Logging
     **************************/
    //println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
    //val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //val jobEndTime = System.nanoTime()
    //logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
    //logger.log(Level.INFO, "JOb span at " + (jobEndTime-jobStartTime)/1000 + "milliseconds")
    /**************************
        Time Logging
     **************************/
  
  
  
    /**************************
        Time Logging
     **************************/
    //val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //val DeltaDebuggingStartTime = System.nanoTime()
    //logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
    /**************************
        Time Logging
     **************************/
  
  
    //val delta_debug = new DDNonExhaustive[String]
    //delta_debug.setMoveToLocalThreshold(local);
    //val returnedRDD = delta_debug.ddgen(lines, new Test, new SequentialSplit[String], lm, fh , DeltaDebuggingStartTime)
  
  
    /**************************
        Time Logging
     **************************/
    //val DeltaDebuggingEndTime = System.nanoTime()
    //val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
    //logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
    //logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")
    /**************************
        Time Logging
     **************************/
  
    //To print out the result
    //    for (tuple <- output) {
    //      println(tuple._1 + ": " + tuple._2)
    //    }
  
    println("Job's DONE!")
  }
  
  def failure(m : Map[String, Int]): Boolean ={
    var fails = false
    for((k,v) <- m){
      if(v > 50) fails = true
    }
    return fails
  }
  def filterSym(str:String): Boolean ={
    val sym: Array[String] = Array(">","<" , "*" , "="  , "#" , "+" , "-" , ":" , "{" , "}" , "/","~" , "1" , "2" , "3" ,"4" , "5" , "6" , "7" , "8" , "9" , "0")
    for(i<- sym){
      if(str.contains(i)) {
        return false;
      }
    }
    return true;
  }
  
  val optionalSleep: () => Unit = if(WITH_ARTIFICIAL_DELAY) {
    () => {Thread.sleep(500)}
  } else {
    () => {}
  }
  
}
