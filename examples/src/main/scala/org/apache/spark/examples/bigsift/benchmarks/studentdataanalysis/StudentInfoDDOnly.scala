package org.apache.spark.examples.bigsift.benchmarks.studentdataanalysis

/**
 * Created by Michael on 4/14/16.
 */

import java.util.Calendar
import java.util.logging._

import org.apache.spark.SparkContext._
import org.apache.spark.examples.bigsift.bigsift.{SequentialSplit, DDNonExhaustive}
import org.apache.spark.{SparkConf, SparkContext}


object StudentInfoDDOnly {
     private val exhaustive = 0

     def main(args: Array[String]): Unit = {
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
          var local  = 0
          if(args.length < 2){
               sparkConf.setAppName("Student_Info")
                    .set("spark.executor.memory", "2g").setMaster("local[6]")
               logFile = "/home/ali/work/temp/git/bigsift/src/benchmarks/studentdataanalysis/datageneration/studentData.txt"
          }else{
               logFile = args(0)
               local  = args(1).toInt
          }
          //set up spark context
          val ctx = new SparkContext(sparkConf)

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
          //spark program starts here
          val records = ctx.textFile(logFile, 1)
          // records.persist()
          val grade_age_pair = records.map(line => {
               val list = line.split(" ")
               (list(4).toInt, list(3).toInt)
          })
          val average_age_by_grade = grade_age_pair.groupByKey
               .map(pair => {
               val itr = pair._2.toIterator
               var moving_average = 0.0
               var num = 1
               while (itr.hasNext) {
                    moving_average = moving_average + (itr.next() - moving_average) / num
                    num = num + 1
               }
               (pair._1, moving_average)
          })

          val out = average_age_by_grade.collect()
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
          //print out the result for debugging purpose
         for (o <- out) {
              println( o._1 + " - " + o._2)

          }


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
          delta_debug.setMoveToLocalThreshold(0);
         val returnedRDD = delta_debug.ddgen(records, new Test, new SequentialSplit[String], lm, fh , DeltaDebuggingStartTime)

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
          println("Job's DONE!")
          ctx.stop()

     }

}