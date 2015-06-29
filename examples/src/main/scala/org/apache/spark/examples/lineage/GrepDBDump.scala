/*
 * Grep workload for BigDataBench
 */
package org.apache.spark.examples.lineage

import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}

object GrepDBDump {

	def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    var lineage = false
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
    var part = 2
    var size = ""
    if(args.size < 3) {
      logFile = "README.md"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      size = args(1).substring(5)
      part = args(2).toInt
    }
    conf.setAppName("Grep-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(true)

    // Job
    val lines = lc.textFile(logFile, part)
    val result = lines.filter(line => line.contains("congress"))
    println(result.count)

    lc.setCaptureLineage(false)

    Thread.sleep(10000)

    // Dumping to DB
    val url="jdbc:mysql://scai15.cs.ucla.edu:3306"
    val username = "root"
    val password = "root"
    val driver = "com.mysql.jdbc.Driver"
   // Class.forName(driver)//.newInstance
    var linRdd = result.getLineage()
//    linRdd.saveAsDBTable(url, username, password, "Trace.tap", driver)
//  //  linRdd.lineageContext.getBackward()
//    linRdd = lines.getLineage()
//    linRdd.saveAsDBTable(url, username, password, "Trace.hadoop", driver)

//    linRdd = result.getLineage()
    linRdd.saveAsCSVFile("tap-" + size)
    linRdd = lines.getLineage()
    linRdd.saveAsCSVFile("hadoop-" + size)
    sc.stop()
	}
}