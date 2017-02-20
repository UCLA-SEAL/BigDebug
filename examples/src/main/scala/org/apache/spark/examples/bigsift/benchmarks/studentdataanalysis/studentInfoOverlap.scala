package org.apache.spark.examples.bigsift.benchmarks.studentdataanalysis

/**
 * Created by Michael on 4/14/16.
 */

import java.util.Calendar
import java.util.logging._

import org.apache.spark.examples.bigsift.bigsift.{SequentialSplit, DDNonExhaustive}
import org.apache.spark.{SparkConf, SparkContext}

//import java.util.List

//remove if not needed

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object studentInfoOverlap {
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

          //start recording lineage time
          val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
          val LineageStartTime = System.nanoTime()
          logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

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

          //print out the result for debugging purpose
          var outlier1 = -1L
          for (o <- out) {
              println(o._1._1 + ": " + o._1._2 + " - " + o._2)
               if(o._1._2 > 25){
                    outlier1 = o._2
               }
          }

          lc.setCaptureLineage(false)
          Thread.sleep(1000)

          var linRdd = average_age_by_grade.getLineage()
          linRdd.collect
          linRdd = linRdd.filter(s => {
               //list.contains(s)
               s == outlier1
          })
          val show1 = linRdd.goBackAll().show(false).toRDD
          println("Error inducing input from first error : " + show1.count())

          // linRdd.collect().foreach(println)
          //   val show1Rdd = linRdd.show().toRDD

          println(">>>>>>>>>>>>>>>>>>>>>>>>>>   First Lineage Tracing done  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

          lc.setCaptureLineage(true)

          //  val lc2 = new LineageContext(ctx)
          // lc2.setCaptureLineage(true)

          // Second run for overlapping lineage
          val major_age_pair = lc.textFile(logFile, 1).map(line => {
               val list = line.split(" ")
               (list(5), list(3).toInt)
          })
          val average_age_by_major = major_age_pair.groupByKey
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

          val out2 = average_age_by_major.collectWithId()

          //print out the result for debugging purpose
          var outlier2 = -1L
          for (o <- out2) {
               println(o._1._1 + ": " + o._1._2 + " - " + o._2)
               if(o._1._2 > 26){
                    outlier2 = o._2
               }
          }


          // lc2.setCaptureLineage(false)
          //stop capturing lineage information
          lc.setCaptureLineage(false)
          Thread.sleep(1000)

          val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
          val DeltaDebuggingStartTime = System.nanoTime()
          logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

          var rdd2 = average_age_by_major.getLineage()
          rdd2.collect
          rdd2 = rdd2.filter(s => {
               s == outlier2
          })
          val show2 = rdd2.goBackAll().show(false).toRDD
          println("Error inducing input from second error : " + show2.count())
        //  val overlap = rdd2.intersection(linRdd).collect
         // val array = overlap.map(s => s.asInstanceOf[(Int, Int)]._2)


          ///***To View Overlapping Data **/
          val mapped = show2.intersection(show1)
          println("Overlapping error inducing inputs from two lineages : " + mapped.count())


          val delta_debug = new DDNonExhaustive[String]
          delta_debug.setMoveToLocalThreshold(local)
         val returnedRDD = delta_debug.ddgen(mapped, new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)

          println(">>>>>>>>>>>>>  DD Done  <<<<<<<<<<<<<<<")
     //     val ss = returnedRDD.collect.foreach(println)

          val DeltaDebuggingEndTime = System.nanoTime()
          val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
          logger.log(Level.INFO, "DeltaDebugging + L  (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
          logger.log(Level.INFO, "DeltaDebugging  + L (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")

          println("Job's DONE!")
          ctx.stop()

     }

}