package org.apache.spark.examples.lineage

/**
 * Created by Michael on 2/3/16.
 */
import java.util.Calendar
import java.util.logging._

import org.apache.spark.{SparkConf, SparkContext}
//import java.util.List

//remove if not needed
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object wordCount_CLDD {
  //    def main(args:Array[String]): Unit = {
  //      val sparkConf = new SparkConf().setMaster("local[8]")
  //      sparkConf.setAppName("WordCount_LineageDD-" )
  //        .set("spark.executor.memory", "2g")
  //
  //      val ctx = new SparkContext(sparkConf)
  //
  //      val lines = ctx.textFile("../textFile", 1)
  //      val constr = new sparkOperations
  //      val output = constr.sparkWorks(lines).collect
  //      for (tuple <- output) {
  //        println(tuple._1 + ": " + tuple._2)
  //      }
  //      ctx.stop()
  //    }

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
      val sparkConf = new SparkConf().setMaster("local[1]")
      sparkConf.setAppName("WordCount_CLDD")
        .set("spark.executor.memory", "2g")

      //set up lineage
      var lineage = true
      var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
      if (args.size < 2) {
        logFile = "README.MD"
        lineage = true
      } else {
        lineage = args(0).toBoolean
        logFile += args(1)
        sparkConf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
      }
      //

      //set up spark context
      val ctx = new SparkContext(sparkConf)


      //set up lineage context
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)
      //

      //Prepare for Hadoop MapReduce - for correctness test only
      /*
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/WordCount1.jar", "WordCount1", "-r", "1", "/Users/Michael/IdeaProjects/InvertedIndex/myLog", "output").!!
      */

      //start recording lineage time
      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      //spark program starts here
      val lines = lc.textFile("README.md", 2)
      val wordCount = lines.flatMap(line => line.trim().replaceAll(" +", " ").split(" ")).filter(_.size > 0)
        .map(word => (word.substring(0, 1), word.length))
        //.reduceByKey(_ + _) //Use groupByKey to avoid combiners
        .groupByKey()
        .map(pair => {
          val itr = pair._2.toIterator
          var returnedValue = 0
          var size = 0
          while (itr.hasNext) {
            val num = itr.next()
            returnedValue += num
            size += 1
          }
          (pair._1, returnedValue/size)
        })
        //this map marks the faulty result
        .map(pair => {
          var value = pair._2.toString
          if (pair._2 > 70) {
            value += "*"
          }
          (pair._1, value)
        })

      val out = wordCount.collectWithId()

      //stop capturing lineage information
      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      //print out the result for debugging purpose
      for (o <- out) {
        println(o._1._1 + ": " + o._1._2 + " - " + o._2)
      }



//      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/WordCount_CLDD/lineageResult"))

      //get the list of lineage id
      var list = List[Long]()
      for (o <- out) {
        if (o._1._2.substring(o._1._2.length - 1).equals("*")) {
          list = o._2 :: list
        }
      }

      var linRdd = wordCount.getLineage()
      linRdd.collect.foreach(println)

      linRdd  = linRdd.filter(s => list.contains(s))
      linRdd = linRdd.goBack()
      linRdd.collect().foreach(println)
      linRdd.show()
      linRdd = linRdd.goBack()
      linRdd.collect().foreach(println)
      linRdd.show()
      linRdd = linRdd.goBack()
      linRdd.collect().foreach(println)
      linRdd.show()

      //At this stage, technically lineage has already find all the faulty data set, we record the time
      val lineageEndTime = System.nanoTime()
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime) / 1000 + " microseconds")
      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)

//      linRdd.show.collect.foreach(s => {
//        pw.append(s.toString)
//        pw.append('\n')
//      })

//      pw.close()


      linRdd = linRdd.goNext()

      linRdd.collect().foreach(println)
      linRdd.show()
      linRdd = linRdd.goNext()

      linRdd.collect().foreach(println)
      linRdd.show()
 //     linRdd.show()
//      linRdd.goNextAll().show()
//      val mappedRDD = showMeRdd.map(s => {
//        val str = s.toString
//        val index = str.lastIndexOf(",")
//        val lineageID = str.substring(index + 1, str.length - 1)
//        val content = str.substring(2, index - 1)
//        val index2 = content.lastIndexOf(",")
//        ((content.substring(0, index2), content.substring(index2 + 1).toInt), lineageID.toLong)
//      })
//
//      println("MappedRDD has " + mappedRDD.count() + " records")
//
//
//
////      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/WordCount_CLDD/lineageResult", 1)
//      //val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/textFile", 1)
//
////      val num = lineageResult.count()
////      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")
//
//
//      //Remove output before delta-debugging
//      val outputFile = new File("/Users/Michael/IdeaProjects/WordCount_CLDD/output")
//      if (outputFile.isDirectory) {
//        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
//      }
//      outputFile.delete
//
//      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
//      val DeltaDebuggingStartTime = System.nanoTime()
//      logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
//
//      /** **************
//        * **********
//        */
//      //lineageResult.cache()
//
//
//
////      if (exhaustive == 1) {
////        val delta_debug = new DD[(String, Int)]
////        delta_debug.ddgen(mappedRDD, new Test,
////          new Split, lm, fh)
////      } else {
//        val delta_debug = new DD_NonEx[(String, Int), Long]
//        val returnedRDD = delta_debug.ddgen(mappedRDD, new Test, new Split, lm, fh)
////      }
//      val ss = returnedRDD.collect
//     // linRdd.collect.foreach(println)
//      linRdd = wordCount.getLineage()
//      linRdd.collect
//      linRdd = linRdd.goBack().goBack().filter(l => {
//        if(l.asInstanceOf[(Int, Int)]._2 == ss(0)._2.toInt){
//          println("*** => " + l)
//          true
//        }else false
//      })
//
//      linRdd = linRdd.goBackAll()
//      linRdd.collect()
//      linRdd.show()
//
//      val DeltaDebuggingEndTime = System.nanoTime()
//      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
//      logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
//      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")
//
//      println("Job's DONE!")
      ctx.stop()
    }
  }
}


