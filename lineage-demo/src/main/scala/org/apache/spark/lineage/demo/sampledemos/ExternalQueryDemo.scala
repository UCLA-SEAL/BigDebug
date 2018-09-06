package org.apache.spark.lineage.demo.sampledemos

import org.apache.ignite.Ignition
import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.demo.LineageBaseApp
import org.apache.spark.lineage.perfdebug.lineageV2.{LineageCacheRepository, LineageWrapper}
import org.apache.spark.lineage.perfdebug.utils.PerfLineageUtils.printRDDWithMessage
import org.apache.spark.rdd.RDD

/**
 * Demo of querying after execution, relying on external cache (in this case, Ignite). It takes in two parameters:
 * 1. Application ID, eg "local-123"
 * 2. Filepaths for hadoop data sources. Note that this should reflect the order in which these RDDs are used within
 * the application. If the same data source is used multiple times, it should appear in this list multiple times as
 * well. This demo does not handle cases when these text files are generated with a specified number of partitions.
 * More explicitly: Each source RDD should be retrievable via the default sparkContext.textFile(_) method.
 *
 * This method will retrieve the appropriate lineage data, retrieve the slowest single record, and trace back to its
 * hadoop inputs.
 */
object ExternalQueryDemo extends LineageBaseApp(sparkLogsEnabled = false) {
  
  def run(lc: LineageContext, args: Array[String]): Unit = {
    val testId = args.head //"local-1531963340265"
    val hadoopFiles: Array[String] = args.drop(1)
    println(s"Running external query demo for ID $testId with ${hadoopFiles.mkString(",")}")
    // demonstration that we can operate purely with the Spark context (+ external ignite dependencies)
    val sc = lc.sparkContext
    val lineage = LineageWrapper.fromAppId(testId)
    
    lineage.printDependencies()
    
    val slowestRecord = lineage.tracePerformance(printDebugging = true,
                                                 printLimit = defaultPrintLimit)
                               .take(1)
    printHadoopSources(slowestRecord, hadoopFiles.map(sc.textFile(_)): _*)
  }
  
}