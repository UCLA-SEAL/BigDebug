package org.apache.spark.examples.bigsift.benchmarks.sequencecount

/**
 * Created by Michael on 11/12/15.
 */


import java.util.logging._
import java.util.{Calendar, StringTokenizer}

import org.apache.spark.examples.bigsift.bigsift.{DDNonExhaustive, SequentialSplit}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


import scala.collection.mutable.MutableList


object TriSequenceCountDDonly {

  private val exhaustive = 0

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

      //start recording time for lineage
      /**************************
        * Time Logging
        **************************/
      val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val jobStartTime = System.nanoTime()
      logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
      /**************************
        * Time Logging
        **************************/
      val lines = ctx.textFile(logFile, 5)
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

      }).reduceByKey(_+_).filter(s => TriSequenceCount.failure(s))

      val out = sequence.collect()

      /**************************
        * Time Logging
        **************************/
      println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
      val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val jobEndTime = System.nanoTime()
      logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
      logger.log(Level.INFO, "JOb span at " + (jobEndTime-jobStartTime)/1000 + "milliseconds")
      /**************************
        * Time Logging
        **************************/


      /**************************
        * Time Logging
        **************************/
      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging   (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
      /**************************
        * Time Logging
        **************************/



      val delta_debug = new DDNonExhaustive[String]
      val test = new Test
      val returnedRDD = delta_debug.ddgen(lines, test, new SequentialSplit[String], lm, fh , DeltaDebuggingStartTime)


      /**************************
        * Time Logging
        **************************/
      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted)  ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted)   takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")
      /**************************
        * Time Logging
        **************************/

      println("Job's DONE! Works - check goNext, incomplete result!:/")
      ctx.stop()
    }
  }


}
