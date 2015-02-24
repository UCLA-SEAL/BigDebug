/*
 * Sort workload for BigDataBench
 */
package org.apache.spark.examples.lineage

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object Sort {

	def main(args: Array[String]): Unit = {
    val logFile = "README.md"
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Sort")
    val sc = new SparkContext(conf)
    //val lc = new LineageContext(sc)
    //lc.setCaptureLineage(true)
		val lines = sc.textFile(logFile, 2)
		val data_map = lines.map(line => {
			(line, 1)
		})
		val result = data_map.sortByKey().map{line => line._1}
		result.collect().foreach(println)
	}
}
