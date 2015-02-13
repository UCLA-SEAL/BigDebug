/*
 * Grep workload for BigDataBench
 */
package org.apache.spark.examples.lineage

import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object Grep {

	def main(args: Array[String]): Unit = {
    //val logFile = "README.md"
    val logFile = "../../datasets/output10.txt"
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Grep")
    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)
    lc.setCaptureLineage(true)
    val lines = lc.textFile(logFile, 2)
    val result = lines.filter(line => line.contains("spark"))
    print(result.collect().size)
		//println(result.collect().mkString("\n"))

//    lc.setCaptureLineage(false)
//    var lineage = result.getLineage()
//    lineage.collect().foreach(println)
//    lineage = lineage.goBack()
//    lineage.collect.foreach(println)
//    lineage.show
    sc.stop()
	}
}