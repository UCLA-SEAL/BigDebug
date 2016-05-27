package org.apache.spark.examples.lineage

import java.util.logging._

import org.apache.spark.{SparkConf, SparkContext}

//remove if not needed
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object FlightProgram {
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
      val sparkConf = new SparkConf().setMaster("local[6]")
      sparkConf.setAppName("Destination")
        .set("spark.executor.memory", "2g")

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

      //set up lineage context and start capture lineage
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)
      //

      //start counting the time
      val LineageStartTime = System.nanoTime()

      //run spark with lineage
      val lines = lc.textFile("/Users/inter/datasets/xaa", 10)
//      val single_hop = lines.map(s => {
//        val list = s.split(",")
//        ((list(2), list(3)), list(5))
//      })
//        .filter(pair => pair._1._1.substring(1, pair._1._1.length - 1).equals("JFK")
//        && pair._1._2.substring(1, pair._1._2.length - 1).equals("DEN"))


      val destination_key = lines.map(s => {
        val list = s.split(",")
        (list(3), (list(2), list(5).toInt))
      })

      val departure_key = lines.map(s => {
        val list = s.split(",")
        (list(2), (list(3), list(5).toInt))
      })


      val double_hop = destination_key//.filter(pair => pair._2._1.substring(1, pair._2._1.length - 1).equals("JFK"))
        .join(departure_key)
      .filter(pair => pair._2._2._1.substring(1, pair._2._2._1.length - 1).equals("PHL"))
            .map(pair => ((pair._2._1._1, pair._2._2._1), pair._2._1._2 * pair._2._2._2))

      //      val triple_hop = destination_key.filter(pair => pair._2._1.substring(1, pair._2._1.length - 1).equals("JFK"))
      //      .join[(String, Int)](departure_key)
      //      .map(pair => (pair._2._2._1, (pair._2._1._1, pair._2._1._2 * pair._2._2._2)))
      //      .join[(String, Int)](departure_key.filter(pair => pair._2._1.substring(1, pair._2._1.length - 1).equals("DEN")))
      //      .map(pair => ((pair._2._1._1, pair._2._2._1), pair._2._1._2 * pair._2._2._2))



      val out = double_hop.collectWithId()

      //stop capturing lineage information
      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      //print out the result for debugging purpose
      var list = List[Long]()
      for (o <- out) {
        if (o._1._2 < 0) {
          println(o._1._1 + ": " + o._1._2 + " - " + o._2)//print out the wrong output for debugging purposes
          list = o._2 :: list
        }
      }

      var linRdd = double_hop.getLineage()
      println(linRdd.count())
      linRdd = linRdd.filter(s => list.contains(s))
      println(linRdd.count())
      linRdd = linRdd.goBackAll()
      println(linRdd.count())
      val showRdd = linRdd.show()
      /*
            //find the index of the data that cause exception
            var list = List[Long]()
            for (o <- out) {
              if (o._1._2 > 100){
                list = o._2 :: list
              }
            }

            var linRdd = average_flight.getLineage()
            linRdd.collect

            linRdd = linRdd.filter( l => {
              //println("***" + l + "***") //debug
              list.contains(l)
            })

            linRdd = linRdd.goBackAll()

            val showMeRdd = linRdd.show()
            val mappedRDD = showMeRdd.map(s => {
              val list = s.split(",")
              (list(0), list(5).toInt)
            })
                  //find the index of the data that cause exception
                  var list = List[Long]()
                  for (o <- out) {
                    if (o._1._2 > 100){
                      list = o._2 :: list
                    }
                  }

                  var linRdd = average_flight.getLineage()
                  linRdd.collect

                  linRdd = linRdd.filter( l => {
                    //println("***" + l + "***") //debug
                    list.contains(l)
                  })

                  linRdd = linRdd.goBackAll()

                  val showMeRdd = linRdd.show()
                  val mappedRDD = showMeRdd.map(s => {
                    val list = s.split(",")
                    (list(0), list(5).toInt)
                  })

                  val delta_debug = new DD_NonEx[(String, Int)]
                  val returnRDD = delta_debug.ddgen(mappedRDD, new Test, new Split, lm, fh)

                  val ss = returnRDD.collect
                  ss.foreach(println)

                  val endTime = System.nanoTime()
                  logger.log(Level.INFO, "Record total time: Delta-Debugging + Linegae + goNext:" + (endTime - LineageStartTime)/1000 + " microseconds")
            */
      println("Job's done!")
      ctx.stop()
    }
  }
}
