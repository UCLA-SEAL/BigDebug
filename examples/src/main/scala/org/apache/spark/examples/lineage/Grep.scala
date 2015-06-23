/*
 * Grep workload for BigDataBench
 */
package org.apache.spark.examples.lineage

import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object Grep {

	def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    var lineage = false
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
    if(args.size < 2) {
      logFile = "README.md"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
//      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
//      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      conf.set("spark.kryo.referenceTracking", "false")
//      conf.set("spark.kryo.registrationRequired", "true")
    }
    conf.setAppName("Grep-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(lineage)

    // Job
    val lines = lc.textFile(logFile, 2)
    val result = lines.filter(line => line.contains("congress"))
    println(result.count)
    //println(result.collect().mkString("\n"))

    lc.setCaptureLineage(false)
    Thread.sleep(10000)
//    // Full Trace backward
//    var linRdd = result.getLineage()
//    linRdd.collect().foreach(println)
//    linRdd = linRdd.goBack()
//    linRdd.collect.foreach(println)
//    linRdd.show
//
//    // Trace backward one record
//    linRdd = result.getLineage()
//    linRdd.collect().foreach(println)
//    linRdd = linRdd.filter(0)
//    linRdd.collect().foreach(println)
//    linRdd = linRdd.goBack()
//    linRdd.collect.foreach(println)
//    linRdd.show

//    for(i <- 1 to 10) {
//      var linRdd = result.getLineage()
//      linRdd.collect //.foreach(println)
//      //    linRdd.show
//      linRdd = linRdd.filter(0)
//      linRdd = linRdd.goBack()
//      linRdd.collect //.foreach(println)
//      println("Done")
//    }

//    // Full Trace forward
//    linRdd = lines.getLineage()
//    linRdd.collect.foreach(println)
//    linRdd.show()
//    linRdd = linRdd.goNext()
//    linRdd.collect.foreach(println)

    // Trace forward one record
    var linRdd = result.getLineage()
    linRdd.collect
    linRdd = linRdd.filter(0)
    linRdd = linRdd.goBack()
    val value = linRdd.take(1)(0)
    for(i <- 1 to 10) {
      var linRdd = lines.getLineage().filter(r => r.asInstanceOf[(Int, Int)] == value)
      linRdd.collect()//.foreach(println)
      //linRdd.show()
      linRdd = linRdd.filter(0)
  //    linRdd = linRdd.filter(r => r.asInstanceOf[(Int, Int)] == value)
      //    linRdd.collect.foreach(println)
      //linRdd.show()
      linRdd = linRdd.goNext()
      linRdd.collect()//.foreach(println)
      println("Done")
    }
    sc.stop()
	}
}