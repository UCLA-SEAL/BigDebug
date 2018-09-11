package org.apache.spark.lineage.demo.sampledemos

import org.apache.ignite.Ignition
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.demo.LineageBaseApp
import org.apache.spark.lineage.perfdebug.lineageV2.{LineageCacheRepository, LineageWrapper}
import org.apache.spark.lineage.perfdebug.utils.PerfLineageUtils.printRDDWithMessage
import org.apache.spark.lineage.rdd.Lineage
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
object ExternalQueryDemo extends LineageBaseApp(
                                                sparkLogsEnabled = false,
                                                lineageEnabled = false
                                                ) {
  
  private object ExecutionMode extends Enumeration {
    val BACKWARD_ALL, BACKWARD_ALL_WITH_HADOOP_INPS, FORWARD_SUM, FORWARD_SUM_AND_LINEAGE_TRACE,
    FORWARD_SUM_AND_LINEAGE_INPUT_JOIN, DEFAULT = Value
  }
  import ExecutionMode._
  
  private val execMode= BACKWARD_ALL
  private var testId: String = _
  private var hadoopFilePaths: Array[String] = _
  
  
  /**
   * Endpoint to override the typical spark configuration.
   */
  override def initConf(args: Array[String], defaultConf: SparkConf): SparkConf = {
    var conf = execMode match {
      case FORWARD_SUM if args.lift(1).contains("/Users/jteoh/Code/Performance-Debug-Benchmarks/StudentInfo/studentData_1M_bias0_0.30.txt") =>
        val mem = "5g"
        println(s"WARNING: ATTEMPTING DIFFERENT DRIVER MEMORY SETTING OF $mem")
        defaultConf.set("spark.driver.memory", mem)
      case _ => defaultConf
    }
    testId = args.head //"local-1531963340265"
    hadoopFilePaths = args.drop(1)
    val specializedAppName = s"${appName}_${execMode}-(${hadoopFilePaths.mkString(",")})-${testId}"
    conf.setAppName(specializedAppName)
  }
  
  def run(lc: LineageContext, args: Array[String]): Unit = {
    println(s"Running external query demo with mode $execMode for ID $testId with " +
              s"${hadoopFilePaths.mkString(",")}")
    // demonstration that we can operate purely with the Spark context (+ external ignite dependencies)
    val sc = lc.sparkContext
    
    val hadoopSourceRDDs = hadoopFilePaths.map(sc.textFile(_))
    
    Lineage.measureTimeWithCallback({
      val lineage = LineageWrapper.fromAppId(testId)
      // lineage.printDependencies()
      execMode match {
        case BACKWARD_ALL =>
          // trace back all and count from each as a low-impact RDD action
          val counts = lineage.traceBackAllSources().map(_.lineageCache.count())
          println(s"Lineage trace counts: $counts")
        case BACKWARD_ALL_WITH_HADOOP_INPS =>
          // Same as BACKWARD_ALL but also join to get hadoop inputs (as opposed to lineage ids)
          val hadoopSourceLineageWrappers = lineage.traceBackAllSources()
          val joinedResults =
            joinHadoopWrappersAndInputs(hadoopSourceLineageWrappers, hadoopSourceRDDs)
          val counts = joinedResults.map(_.count())
          println(s"Lineage trace + hadoop join counts: $counts")
        case FORWARD_SUM =>
          val perf = lineage.tracePerformance(printDebugging = false,
                                              printLimit = defaultPrintLimit)
          // Count to force a low-impact RDD action
          val count = perf.perfCache.count()
          println(s"Forward sum count: $count")
        case FORWARD_SUM_AND_LINEAGE_TRACE =>
          val perf = lineage.tracePerformance(printDebugging = false,
                                              printLimit = defaultPrintLimit)
          // implicitly forces a takeOrdered - this is on the wrapper, not on the RDD.
          val slowestRecord = perf.take(1)
          val hadoopSourceLineageWrappers = slowestRecord.traceBackAllSources()
          val counts = hadoopSourceLineageWrappers.map(_.lineageCache.count())
          println(s"Lineage trace counts: $counts")
        case FORWARD_SUM_AND_LINEAGE_INPUT_JOIN =>
          val perf = lineage.tracePerformance(printDebugging = false,
                                              printLimit = defaultPrintLimit)
          // implicitly forces a takeOrdered - this is on the wrapper, not on the RDD.
          val slowestRecord = perf.take(1)
          val hadoopSourceLineageWrappers = slowestRecord.traceBackAllSources()
          val joinedResults =
            joinHadoopWrappersAndInputs(hadoopSourceLineageWrappers, hadoopSourceRDDs)
          val counts = joinedResults.map(_.count())
          println(s"Perf trace + hadoop join counts: $counts")
        case DEFAULT =>
          val perf = lineage.tracePerformance(printDebugging = true,
                                              printLimit = defaultPrintLimit)
          val slowestRecord = perf.take(1)
          printHadoopSources(slowestRecord, hadoopSourceRDDs: _*)
        case _ =>
          throw new IllegalArgumentException("UNKNOWN MODE")
      }
    }, latency => println(s"Execution-only latency: $latency ms"))
    
  }
}