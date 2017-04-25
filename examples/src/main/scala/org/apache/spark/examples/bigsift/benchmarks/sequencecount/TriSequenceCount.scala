package org.apache.spark.examples.bigsift.benchmarks.sequencecount

/**
 * Created by Michael on 11/12/15.
 */

import java.util.logging._
import java.util.{Calendar, StringTokenizer}

import org.apache.spark.examples.bigsift.bigsift.{SequentialSplit, DDNonExhaustive}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.MutableList

//remove if not needed

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object TriSequenceCount {
  def main(args: Array[String]): Unit = {
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
      if(args.length < 2) {
        sparkConf.setMaster("local[6]")
        sparkConf.setAppName("TermVector_LineageDD").set("spark.executor.memory", "2g")
        logFile =  "/home/ali/work/temp/git/bigsift/src/benchmarks/termvector/data/textFile"
      }else{
        logFile = args(0)
        local  = args(1).toInt
      }

      //set up spark context
      val ctx = new SparkContext(sparkConf)

      //set up lineage context and start capture lineage
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(true)


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
      val lines = lc.textFile(logFile, 5)

      val sequence = lines.filter(s => TriSequenceCount.filterSym(s)).flatMap(s => {

        var wordStringP1 = new String("")
        var wordStringP2 = new String("")
        var wordStringP3 = new String("")

        val sequenceList: MutableList[(String, Int)] = MutableList()
       // val colonIndex = s.lastIndexOf(':')
        //val docName = s.substring(0, colonIndex)
        val contents = s
        val itr = new StringTokenizer(contents)
        while (itr.hasMoreTokens) {
          wordStringP1 = wordStringP2
          wordStringP2 = wordStringP3
          wordStringP3 = itr.nextToken
          if (wordStringP1.equals("")) {
            //Do nothing if not all three have values
          }
          else {
            val finalString = wordStringP1 + "|" + wordStringP2 + "|" + wordStringP3 //+ "|" + docName
            if (s.contains("Romeo and Juliet") && finalString.contains("He|has|also"))
              sequenceList += Tuple2(finalString, Int.MinValue)
            else
              sequenceList += Tuple2(finalString, 1)
          }
        }
        sequenceList.toList

      })//.reduceByKey(_+_)
        .reduceByKey(_+_).filter(s => TriSequenceCount.failure(s))

      /**Annotating bugs on cluster**/
     val out =   sequence.collectWithId()
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
      //stop capturing lineage information
      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      //print out the result for debugging purposes
      for (o <- out) {
      println(o._1._1 + ": " + o._1._2 + " - " + o._2)

      }
      //list of bad inputs
      var list = List[Long]()
      for (o <- out) {
          list = o._2 :: list
      }


      /**************************
        Time Logging
        **************************/
      val lineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val lineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + lineageStartTimestamp)
      /**************************
        Time Logging
        **************************/


      var linRdd = sequence.getLineage()
      linRdd.collect

      linRdd = linRdd.filter { l => list.contains(l)}
      linRdd = linRdd.goBackAll()

      val mappedRDD = linRdd.show(false).toRDD

      /**************************
        Time Logging
        **************************/
      println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
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
      delta_debug.setMoveToLocalThreshold(local)
      val returnedRDD = delta_debug.ddgen(mappedRDD, new Test, new SequentialSplit[String], lm, fh , DeltaDebuggingStartTime)
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



      ctx.stop()
    }
  }

  def failure(record:(String, Int)): Boolean = {
    record.asInstanceOf[Tuple2[String , Int]]._2 <= 0
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

