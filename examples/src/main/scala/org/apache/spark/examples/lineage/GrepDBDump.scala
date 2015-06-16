/*
 * Grep workload for BigDataBench
 */
package org.apache.spark.examples.lineage

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}

object GrepDBDump {

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
    }
    conf.setAppName("Grep-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(lineage)

    // Job
    val lines = lc.textFile(logFile, 2)
    val result = lines.filter(line => line.contains("spark"))
    println(result.count)

    lc.setCaptureLineage(false)

    // Dumping to DB
    val url="jdbc:mysql://localhost:3306"
    val username = "root"
    val password = "root"
    Class.forName("com.mysql.jdbc.Driver").newInstance
    var linRdd = result.getLineage()
    linRdd.saveAsDBTable(url, username, password, "Trace.tap")
    linRdd.lineageContext.getBackward()
    linRdd = linRdd.lineageContext.getCurrentLineagePosition.get
    linRdd.saveAsDBTable(url, username, password, "Trace.hadoop")

    sc.stop()
	}
}