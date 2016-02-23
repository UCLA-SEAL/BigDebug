package org.apache.spark.examples.lineage
/**
 * Created by Michael on 11/23/15.
 */
import java.util.logging._
import java.util.{Calendar, StringTokenizer}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.MutableList

//remove if not needed
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

import scala.util.control.Breaks._
object AdjList {
  val LIMIT: Int = 200000
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
      val sparkConf = new SparkConf().setMaster("local[8]")
      sparkConf.setAppName("AdjacencyList_FaultSeeding")
        .set("spark.executor.memory", "4g")

      //set up lineage
      var lineage = true
      var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
      if (args.size < 2) {
        logFile = "test_log"
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
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/AdjList.jar", "org.apache.hadoop.examples.AdjList", "-m", "3", "-r", "1", "/Users/Michael/IdeaProjects/AdjacencyList/edges_31", "output").!!
      */

      //start recording linege tijme
      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      //spark program starts here

      val lines = lc.textFile("/Users/inter/Develop/spark-lineage-bdd/AdjacencyList/edges_31", 1)
      logger.log(Level.INFO, "Total data set size is " + lines.count)

      val resultEdges = lines.filter(s => {
        val index = s.lastIndexOf(",")
        if (index == -1) false
        else true
      })
        .flatMap(s => {
        val listOfEdges: MutableList[(String, String)] = MutableList()
        val index = s.lastIndexOf(",")
        val outEdge = s.substring(0, index)
        val inEdge = s.substring(index + 1)
        val outList = "from{" + outEdge + "}:to{}"
        var inList = "from{}:to{" + inEdge + "}"
        val out = Tuple2(outEdge, inList)
        val in = Tuple2(inEdge, outList)
        listOfEdges += out
        listOfEdges += in
        listOfEdges
      })
        .groupByKey()
        .map(pair => {
        var fromList: MutableList[String] = MutableList()
        var toList: MutableList[String] = MutableList()
        var fromLine = new String()
        var toLine = new String()
        var vertex = new String()
        //       var itr:util.Iterator[String] = null
        //        try {
        val itr = pair._2.toIterator
        //       }catch{
        //         case e:Exception =>
        //           println("**************************")
        //       }
        while (itr.hasNext) {
          breakable {
            val str = itr.next()
            val strLength = str.length
            val index = str.indexOf(":")
            if (index == -1) {
              println("Wrong input: " + str)
              break
            }
            if (index > 6) {
              fromLine = str.substring(5, index - 1)
            }
            if (index + 5 < strLength) {
              toLine = str.substring(index + 4, strLength - 1)
            }
            if (!fromLine.isEmpty) {
              val itr2 = new StringTokenizer(fromLine, ",")
              while (itr2.hasMoreTokens) {
                vertex = new String(itr2.nextToken())
                if (!fromList.contains(vertex) && fromList.size < LIMIT) {
                  fromList += vertex
                }
              }
            }
            if (!toLine.isEmpty) {
              val itr2 = new StringTokenizer(toLine, ",")
              while (itr2.hasMoreTokens) {
                vertex = new String(itr2.nextToken())
                if (!toList.contains(vertex) && toList.size < LIMIT) {
                  toList += vertex
                }
              }
            }
          }
        }
        fromList = fromList.sortWith((a, b) => if (a < b) true else false)
        toList = toList.sortWith((a, b) => if (a < b) true else false)
        var fromList_str = new String("")
        var toList_str = new String("")
        for (r <- 0 until fromList.size) {
          if (fromList_str.equals("")) fromList_str = fromList(r)
          else fromList_str = fromList_str + "," + fromList(r)
        }
        for (r <- 0 until toList.size) {
          if (toList_str.equals("")) toList_str = toList(r)
          else toList_str = toList_str + "," + toList(r)
        }
        val outValue = new String("from{" + fromList_str + "}:to{" + toList_str + "}")
        (pair._1, outValue)
      })
        //this map marks the faulty result
        .map(pair => {
        val index = pair._2.lastIndexOf(":")
        val substr = pair._2.substring(index + 4, pair._2.length - 1)
        val ll = substr.split(",")
        var value = pair._2
        if (ll.size > 438) {
          value += "*"
        }
        (pair._1, value)
      })

      val output_result = resultEdges.collectWithId()

      //stop capturing lineage information
      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      //print out the result for debugging purpose
      for (o <- output_result) {
        println(o._1._1 + ": " + o._1._2 + " - " + o._2)
      }

      //val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/AdjList_FaultSeeding/lineageResult"))

      //find the lineage id of faulty results
      var listl = List[Long]()
      for (o <- output_result) {
        if (o._1._2.substring(o._1._2.length - 1).equals("*")) {
          listl = o._2 :: listl
        }
      }

      //print out the resulting list for debugging purposes
      println("*************************")
      for (l <- listl) {
        println(l)
      }
      println("*************************")

      var linRdd = resultEdges.getLineage()
      linRdd.collect

      linRdd = linRdd.filter(l => {
        //println(l)
        listl.contains(l)
      })
      linRdd.collect.foreach(println)
      linRdd = linRdd.goBackAll()

      //At this stage, technically lineage has already find all the faulty data set, we record the time
      val lineageEndTime = System.nanoTime()
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime)/1000 + " milliseconds")
      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)

      linRdd.show

      /*
      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/AdjList_FaultSeeding/lineageResult", 1)
      //val lineageResult = ctx.textFile("../AdjacencyList/edges_31", 1)

      val num = lineageResult.count()
      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")

      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/AdjList_FaultSeeding/output")
      if (outputFile.isDirectory) {
        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
      }
      outputFile.delete


      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

      /* *****************
       * *************
       */
      //lineageResult.cache()

      if (exhaustive == 1) {
        val delta_debug: DD[String] = new DD[String]
        delta_debug.ddgen(lineageResult, new Test,
          new Split, lm, fh)
      } else {
        val delta_debug: DD_NonEx[String] = new DD_NonEx[String]
        delta_debug.ddgen(lineageResult, new Test, new Split, lm, fh)
      }

      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime)/1000 + " microseconds")


      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }
*/
      println("Job's DONE!")
      ctx.stop()

    }
  }
}
