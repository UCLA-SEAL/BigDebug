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
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/"
    if(args.size < 2) {
      //logFile = "../../datasets/output.txt"
      //val logFile = "../../datasets/data-MicroBenchmarks/lda_wiki1w_2"
      logFile = "README.md"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("Grep-" + lineage + "-" + logFile)
    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)
    lc.setCaptureLineage(lineage)
    val lines = lc.textFile(logFile, 2)
    val result = lines.filter(line => line.contains("spark"))
    //print(result.count())
		println(result.collect().mkString("\n"))

    lc.setCaptureLineage(false)
    var linRdd = result.getLineage()
    linRdd.collect().foreach(println)
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goNext()
    sc.stop()
	}
}