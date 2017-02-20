package org.apache.spark.examples.bigsift.benchmarks.invertedindex

import java.util.Calendar
import java.util.logging._

import org.apache.spark.examples.bigsift.bigsift.{SequentialSplit, DDNonExhaustive}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.MutableList

/**
 * Created by ali on 12/2/16.
 */

object InvertedIndex
{

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
      if (args.length < 2) {
        sparkConf.setMaster("local[6]")
        sparkConf.setAppName("Inverted Index").set("spark.executor.memory", "2g")
        logFile = "/home/ali/work/temp/git/lineageDelta/spark-lineage/examples/src/main/scala/org/apache/spark/examples/bigsift/benchmarks/invertedindex/data/inverted"
      } else {
        logFile = args(0)
        local = args(1).toInt
      }
      //set up lineage
      var lineage = true
      lineage = true

      val ctx = new SparkContext(sparkConf)
      //start recording time for lineage
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)
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
      val wordDoc = lines.flatMap(s => {
        val wordDocList: MutableList[(String, String)] = MutableList()
        val colonIndex = s.lastIndexOf("^")
        val docName = s.substring(0, colonIndex).trim()
        val content = s.substring(colonIndex + 1)
        val wordList = content.trim.split(" ")
        for (w <- wordList) {
          wordDocList += Tuple2(w, docName)
        }
        wordDocList.toList
      })
        .filter(r => InvertedIndex.filterSym(r._1))
        .groupByKey()
        .map(pair => {
        val docSet = scala.collection.mutable.Set[String]()
        var value = new String("")
        val itr = pair._2.toIterator
        var word = pair._1
        while (itr.hasNext) {
          val doc = itr.next()
          docSet += doc
          /** ******** Bug Seeding *********************/
          if (doc.contains("hdfs://scai01.cs.ucla.edu:9000/clash/datasets/bigsift/wikipedia_50GB/file202") && word.equals("is")) {
            word += "*$*"
          }
          /*********************************************/
        }
        (word, docSet)
      }).filter(s => InvertedIndex.failure(s._1))
      val output = wordDoc.collectWithId()
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

      var list = List[Long]()
      for (o <- output) {
          println(o._1._1 + ": " + o._1._2 + " - " + o._2)
          list = o._2 :: list
      }
      /** ************************
        * Time Logging
        * *************************/
      val lineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val lineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Lineage starts at " + lineageStartTimestamp)
      /** ************************
        * Time Logging
        * *************************/

      var linRdd = wordDoc.getLineage()
      linRdd.collect
      linRdd = linRdd.filter { l => list.contains(l) }
      linRdd = linRdd.goBackAll()
      val showMeRdd = linRdd.show(false).toRDD

      /** ************************
        * Time Logging
        * *************************/
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val lineageEndTime = System.nanoTime()
      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)
      logger.log(Level.INFO, "Lineage span at " + (lineageEndTime - lineageStartTime) / 1000 + "milliseconds")

      /** ************************
        * Time Logging
        * *************************/


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
      delta_debug.setMoveToLocalThreshold(local);
      val returnedRDD = delta_debug.ddgen(showMeRdd, new Test, new SequentialSplit[String], lm, fh, DeltaDebuggingStartTime)

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

      println("Job's DONE! WORKS!")
      ctx.stop()

    }
  }

  def failure(record: String): Boolean ={
        record.endsWith("*$*")
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