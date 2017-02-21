package org.apache.spark.examples.bigsift.benchmarks.termvector

/**
 * Created by Michael on 1/25/16.
 */

import java.util.Calendar
import java.util.logging._

import org.apache.spark.examples.bigsift.bigsift.{SequentialSplit, DDNonExhaustive}
import org.apache.spark.{SparkConf, SparkContext}

//remove if not needed

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object TermVector {


  private val exhaustive = 0

  def sortByValue(map: Map[String, Int]): Map[String, Int] = {
    val list: List[(String, Int)] = map.toList.sortWith((x, y) => {
      if (x._2 > y._2) true
      else false
    })
    list.toMap
  }

  def main(args: Array[String]): Unit = {
    try {
      //set up logger
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
      if(args.length < 2) {
        sparkConf.setMaster("local[6]")
        sparkConf.setAppName("TermVector_LineageDD").set("spark.executor.memory", "2g")
        logFile =  "/home/ali/work/temp/git/bigsift/src/benchmarks/termvector/data/textFile"
      }else{

        logFile = args(0)
        local  = args(1).toInt

      }
      //set up lineage
      var lineage = true
      lineage = true

      val ctx = new SparkContext(sparkConf)

      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)

      //start recording time for lineage
      /**************************
        Time Logging
        **************************/
      val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val jobStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
      /**************************
        Time Logging
        **************************/

      val lines = lc.textFile(logFile, 1)

      val wordDoc = lines
           .map(s => {
        var wordFreqMap: Map[String, Int] = Map()
        val colonIndex = s.indexOf(":")
        val docName = s.substring(0, colonIndex)
        val content = s.substring(colonIndex + 1)
        val wordList = content.trim.split(" ")
        for (w <- wordList) {
          if(TermVector.filterSym(w)){
          if (wordFreqMap.contains(w)) {
            val newCount = wordFreqMap(w) + 1
            /**** Seeding Error***/
            if (newCount > 10) {
              wordFreqMap = wordFreqMap updated(w, 10000)
            }
            /*********************/
            else
              wordFreqMap = wordFreqMap updated(w, newCount)
          } else {
              wordFreqMap = wordFreqMap + (w -> 1)
          }
          }
        }
        // wordFreqMap = wordFreqMap.filter(p => p._2 > 1)
        (docName, wordFreqMap)
      })
           .filter(pair => {
        if (pair._2.isEmpty) false
        else true
      }).reduceByKey{ (v1, v2) =>
        var map: Map[String, Int] = Map()
        map = v1
        var returnMap : Map[String, Int] = Map()
        for((k,v) <- v2){
          if(map.contains(k)){
            val count = map(k)+ v
            map = map updated(k, count)
          }else{
            map = map + (k -> 1)
          }
          }
        map
      }.filter(s => TermVector.failure(s._2))
      val out = wordDoc.collectWithId()

      /**************************
        Time Logging
        **************************/
      println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
      val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val jobEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (jobEndTime-jobStartTime)/1000 + "milliseconds")
      /**************************
        Time Logging
        **************************/


      //stop the lineage capture
      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      //print out the output for debugging purpose
      //	for (o <- out) {
      //	println(o._1._1 + " : " + o._1._2 + " - " + o._2)
      //	}


      var list = List[Long]()
      for (o <- out) {
          println(o._1._1 + " : " + o._1._2 + " - " + o._2)
          list = o._2 :: list

      }

      //print the list for debugging
      //      println("****************************")
      //      for (l <- list) {
      //        println(l)
      //      }
      //      println("****************************")

      /**************************
        Time Logging
        **************************/
      val lineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val lineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + lineageStartTimestamp)
      /**************************
        Time Logging
        **************************/

      var linRdd = wordDoc.getLineage()
      linRdd.collect

      linRdd = linRdd.filter { l => {
        //println("***" + l + "***") //debug
        list.contains(l)
      }
      }

      linRdd = linRdd.goBackAll()
      val showMeRdd = linRdd.show(false).toRDD

      /**************************
        Time Logging
        **************************/
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val lineageEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + lineageEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (lineageEndTime-lineageStartTime)/1000 + "milliseconds")
      /**************************
        Time Logging
        **************************/



      /**************************
        Time Logging
        **************************/
      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
      /**************************
        Time Logging
        **************************/



      val delta_debug = new DDNonExhaustive[String]
      delta_debug.setMoveToLocalThreshold(local);
      val returnedRDD = delta_debug.ddgen(showMeRdd, new Test, new SequentialSplit[String], lm, fh , DeltaDebuggingStartTime)


      /**************************
        Time Logging
        **************************/
      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")
      /**************************
        Time Logging
        **************************/

      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }

      println("Job's DONE! Works!")
      ctx.stop()

    }
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

}

