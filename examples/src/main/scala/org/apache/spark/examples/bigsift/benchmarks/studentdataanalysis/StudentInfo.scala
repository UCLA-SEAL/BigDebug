package org.apache.spark.examples.bigsift.benchmarks.studentdataanalysis

/**
 * Created by Michael on 4/14/16.
 */

import java.util.Calendar
import java.util.logging._

import org.apache.spark.examples.bigsift.bigsift.{DDNonExhaustive, SequentialSplit}
import org.apache.spark.{SparkConf, SparkContext}

//import java.util.List

//remove if not needed

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object StudentInfo {
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
          var local = 500
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

          //set up lineage context
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

          //spark program starts here
          val records = lc.textFile(logFile, 1)
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

          val out = average_age_by_grade.collectWithId()
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
          var outlier = -1L
          for (o <- out) {
              println(o._1._1 + ": " + o._1._2 + " - " + o._2)
               if(o._1._2 > 25){
                    outlier = o._2
               }
          }

          lc.setCaptureLineage(false)
          Thread.sleep(1000)


          /**************************
        Time Logging
            **************************/
          val lineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
          val lineageStartTime = System.nanoTime()
          logger.log(Level.INFO, "JOb starts at " + lineageStartTimestamp)
          /**************************
        Time Logging
            **************************/
          var linRdd = average_age_by_grade.getLineage()
          linRdd.collect
          linRdd = linRdd.filter(s => {
               s == outlier
          })
          val show1 = linRdd.goBackAll().show(false).toRDD
          println("Error inducing input from first error : " + show1.count())


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
          delta_debug.setMoveToLocalThreshold(local)
         val returnedRDD = delta_debug.ddgen(show1, new Test, new SequentialSplit[String], lm, fh , DeltaDebuggingStartTime)

          println(">>>>>>>>>>>>>  DD Done  <<<<<<<<<<<<<<<")
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