package org.apache.spark.examples.bigsift.benchmarks.histogrammovies

import java.util.{StringTokenizer, Calendar}
import java.util.logging._

import org.apache.spark.examples.bigsift.bigsift.{SequentialSplit, DDNonExhaustive}
import org.apache.spark.lineage.LineageContext

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkContext, SparkConf}

class BugLocationID {

    def main(args: Array[String]) {

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
          logFile = "/home/ali/work/temp/git/bigsift/src/benchmarks/histogrammovies/data/file1s.data"
        } else {

          logFile = args(0)
          local = args(1).toInt

        }
        //set up lineage
        var lineage = true
        lineage = true

        val sc = new SparkContext(sparkConf)

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

        val lines = sc.textFile("hdfs://scai01.cs.ucla.edu:9000/clash/datasets/bigsift/kmeans_30GB/*", 1)
        val count = lines.count();
        lines.zipWithIndex().map{s => (s._1 ,  s._2*100/count)}.filter( s=> s._2 % 10 == 9 ).map{
         s =>
           var movieStr:String =""
          val  movieIndex = s._1.indexOf(":")
          if (movieIndex > 0) {
            movieStr = s._1.substring(0, movieIndex)
          }
           ( s._2, movieStr)
        }.groupByKey().map(s => (s._1 , s._2.head)).collect().foreach(println)


      //Compute once first to compare to the groundTruth to trace the lineage
      }
  }


/** *********
  *
  *
  *(0,2691)
(10,7497450000)
(20,12442950000)
(30,4627160000)
(40,9815760000)
(50,269170000)
(60,5330570000)
(70,10821670000)
(80,66354)
(90,123249)


  (9,6687750000)
(19,11949150000)
(29,4109360000)
(39,9294460000)
(49,12714260000)
(59,4753370000)
(69,9905770000)
(79,61066)
(89,112898)
(99,3469750000)


  *
  *
  *
  *
  */
