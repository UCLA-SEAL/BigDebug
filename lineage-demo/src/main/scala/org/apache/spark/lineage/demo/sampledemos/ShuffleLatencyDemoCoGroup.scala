package org.apache.spark.lineage.demo.sampledemos

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.demo.LineageBaseApp
import org.apache.spark.lineage.perfdebug.lineageV2.LineageWrapper._
import org.apache.spark.lineage.perfdebug.perftrace.{AggregateLatencyStats, AggregateStatsStorage, DefaultPerfLineageWrapper}
import org.apache.spark.lineage.rdd.Lineage

/** Intended to illustrate shuffle agg latency statistics in use. This is similar to
 * [[ShuffleLatencyDemoReduce]] except that it uses cogroup rather than reduceByKey.
 * TODO add better description, checks.
 */
object ShuffleLatencyDemoCoGroup extends LineageBaseApp(rewriteAllHadoopFiles = true,
                                                     defaultPrintLimit = None,
                                                     sparkLogsEnabled = false) {
  // Make note of limitation: precogroup (and non-agg preshuffle) is distributing latency across
  // all inputs, even though they're likely keyed later.
  val specialKey = "foo"
  val otherKeyPrefix = "bar"
  val traceLineagePerformance = true
  
  def run(lc: LineageContext, args: Array[String]): Unit = {
    
    val firstRDDPartitionCount = 5
    // TODO these parameters are very fragile, dependent on how hadoop splits inputs!
    val firstRDD = buildFirstRDD(firstRDDPartitionCount, 10) // all 50=specialKey
    val secondRDDPartitionCount = 2
    val secondRDD = buildSecondRDD(secondRDDPartitionCount, 5) // each is unique key
    
    val cogroupRDD = firstRDD.keyBy(identity)
                     .map(x => {
                       Thread.sleep(20)
                       x
                     }).cogroup(secondRDD.keyBy(identity))
    
    printRDDWithMessage(cogroupRDD, "CoGroup result")
    // println(counts.toDebugString)
    // Thread.sleep(1000)
    printAggStats(17, firstRDDPartitionCount) //precogroup for firstRDD
    printAggStats(16, secondRDDPartitionCount) //precogroup for secondRDD
    printAggStats(15, cogroupRDD.getNumPartitions) //postcogroup
    
    println(cogroupRDD.toDebugString)
    val lineageWrapper = cogroupRDD.lineageWrapper
    lineageWrapper.printDependencies(showBefore = false)
    
    val perfWrapper = lineageWrapper.tracePerformance(printDebugging = traceLineagePerformance)
    printRDDWithMessage(perfWrapper.dataRdd.sortBy(_._1),
                        "Performance data", limit = None)
    
    val slowestRecordWrapper = perfWrapper.take(1)
    
    printHadoopSources(slowestRecordWrapper, firstRDD, secondRDD)
  }
  
  var _hasSleptForAggStats = false // agg stats are finalized via threadpool, so doing them
  // immediately is risky. Sleep a bit first to ensure that the results have been uploaded.
  private def printAggStats(rddId: Int, numPartitions: Int): Unit = {
    
    val statsMap: Map[Int, AggregateLatencyStats] = getAggStats(rddId, numPartitions)
    val sep = "-" * 10 + s"$rddId (parts printed: $numPartitions)" + "-" * 10
    println(sep)
    statsMap.toSeq.sortBy(_._1).foreach {
      case (partition, stats) => println(s"$partition: $stats")
    }
    println(sep)
  }
  
  private def getAggStats(rddId: Int, numPartitions: Int) = {
    if(!_hasSleptForAggStats) {
      Thread.sleep(5000)
      _hasSleptForAggStats = true
    }
    AggregateStatsStorage.getInstance().getAllAggStats(appId, rddId, numPartitions)
  }
  
  /** First RDD is simple - it's only one key, with however many partitions/recs per partition is
   *  specified.
   */
  private def buildFirstRDD(numPartitions: Int, partitionSize: Int): Lineage[String] = {
    val data: Seq[String] = Seq.fill(numPartitions * partitionSize)(specialKey)
    buildRDDWithPartitionAssert("firstRDD", data, numPartitions)
  }
  
  /** Second RDD consists of one key per partition. The key for the first partition will be the
   * special key. No key appears in more than  one partition.
   * @param numPartitions
   * @param partitionSize
   * @return
   */
  private def buildSecondRDD(numPartitions: Int, partitionSize: Int) = {
    val data: Seq[String] = Seq.tabulate(numPartitions * partitionSize)(idx => {
      if(idx == 0) specialKey else otherKeyPrefix + idx
    })
    buildRDDWithPartitionAssert("secondRDD", data, numPartitions)
  }
  
  private def buildRDDWithPartitionAssert(name: String,
                                          data: Seq[String],
                                          numPartitions: Int
                                         ): Lineage[String] = {
    println(s"$name data($numPartitions partitions): $data")
    // this program heavily relies on distribution of data!
    val rdd = tempHadoopRDD(name, data,
                            minPartitions = Some(numPartitions),
                            requireExactPartitionCount = true)
    rdd
  }
}
