package org.apache.spark.lineage.demo.sampledemos

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.demo.LineageBaseApp
import org.apache.spark.lineage.perfdebug.lineageV2.LineageWrapper._
import org.apache.spark.lineage.perfdebug.perftrace.{AggregateLatencyStats, AggregateStatsStorage, DefaultPerfLineageWrapper}
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, PartitionWithRecId}
import org.apache.spark.lineage.rdd.Lineage

/** Intended to illustrate shuffle agg latency statistics in use. In practice, this is a simple
 * reduceByKey operation on a carefully constructed dataset such one special key appears in every
 * partition, and makes up all of one of those partitions. The reduceByKey operation should
 * observe that:
 * 1. Map-side combine: all inputs correspond to a single output for one partition - that
 * particular key should have much higher latency when computing map-side latencies.
 * 2. Reduce-side: special key should have the most inputs, and thus also receive the largest
 * proportion of its postshuffle partition latency.
 * This class contains some simple assertions to reflect the expected behavior, within acceptable
 * thresholds.
 */
object ShuffleLatencyDemoReduce extends LineageBaseApp(rewriteAllHadoopFiles = true) {
  val specialKey = "foo"
  val otherKeyPrefix = "bar"
  val traceLineagePerformance = false
  
  def run(lc: LineageContext, args: Array[String]): Unit = {
    
    val numPartitions = 6 // TODO these are very fragile, dependent on how hadoop splits inputs!
    val partitionSize = 10
    val baseRdd: Lineage[String] = buildBaseRDD(numPartitions, partitionSize)
    
    val counts = baseRdd.map((_, 1)).reduceByKey(_ +_)
    printRDDWithMessage(counts, "Simple word count result")
    // println(counts.toDebugString)
    // Thread.sleep(1000)
    printAggStats(7, numPartitions) //preshuffle
    printAggStats(8, counts.getNumPartitions) //postshuffle
    
    
    val lineageWrapper = counts.lineageWrapper
    lineageWrapper.printDependencies(showBefore = true)
    
    val perfWrapper = lineageWrapper.tracePerformance(printDebugging = traceLineagePerformance)
    printRDDWithMessage(perfWrapper.dataRdd.sortBy(_._1),
                        "Performance data", limit = None)
    
    val slowestRecordWrapper = perfWrapper.take(1)
    
    printHadoopSources(slowestRecordWrapper, baseRdd)
    
    /** Due to the way we've constructed this dataset, there's a set of results we expect!
     *  Here's the set of assertions we make:
     *  1. [lineage+perf] The slowest output record back should correspond to the special key
     *  2. [lineage] When traced backwards, this should result in exactly the input records
     *  consisting of the special key.
     *  3. [perf] As there is very little actual computation in non-shuffle processes, each
     *  output record's latency should be very close to the shuffle-based latency using the agg
     *  stats partitions.
     *  3.a: The specialKey will correspond to the agg stats where inputs != outputs .
     *  3.b: All other keys are difficult to map to the exact partition, so we average their
     *  latencies and check within a threshold - this is acceptable if we assume that overall
     *  partition performance is fairly uniform (which is the observed case in local execution).
     *  3.b note: not implemented, because it's slightly more complex than expected. There are
     *  some records that share the same postshuffle partition as the special key, meaning their
     *  end latencies are much smaller since we observe that in practice the number of inputs
     *  doesn't have a major effect for this particular computation (11 inputs vs 7 inputs for a
     *  given partition still has about the same performance). Assertion for 3a is the crucial
     *  one in the computation anyways.
     */
    
    
    val rawRecords: Array[(Long, String)] = slowestRecordWrapper.traceBackAll().joinInputTextRDD(baseRdd).collect()
    assert(rawRecords.forall(_._2 == specialKey)) // #1
    assert(rawRecords.length == (1 * partitionSize) + (numPartitions-1) * 1) // #2
    
    val perfRecords: Array[(PartitionWithRecId, (CacheValue, Long))] = perfWrapper.dataRdd.collect()
    val preShuffleAggStats: Map[Int, AggregateLatencyStats] = getAggStats(7, numPartitions)
    val postShuffleAggStats: Map[Int, AggregateLatencyStats] = getAggStats(8, counts.getNumPartitions)
    val (specialPreShufflePartitions, regularPreShufflePartitions) =
      preShuffleAggStats.values.partition(stats => stats.numInputs != stats.numOutputs)
    val (specialPostShufflePartitions, regularPostShufflePartitions) =
      postShuffleAggStats.values.partition(stats => stats.numInputs != stats.numOutputs)
    
    val maxLatency = perfRecords.map(_._2._2).max // should be specialKey
    require(specialPreShufflePartitions.size == 1, "There should only be one agg stats partition " +
      "corresponding to special key")
    require(specialPostShufflePartitions.size == 1, "There should only be one agg stats partition" +
      " " +
      "corresponding to special key")
    val specialPreShufflePartition = specialPreShufflePartitions.head
    val specialPostShufflePartition = specialPostShufflePartitions.head
    // estimation: specialKey should take up all of preshuffle, and contribute one input per
    // partition in postshuffle; we scale the postshuffle latency accordingly.
    val specialKeyEstLatency = specialPreShufflePartition.latency + (specialPostShufflePartition
                                                                     .latency * numPartitions / specialPostShufflePartition.numInputs)
    val acceptedError = 0.03
    // 3a
    assert(maxLatency >= (specialKeyEstLatency * (1 - acceptedError)), "Special key latency was " +
      s"much lower than expected: $maxLatency < $specialKeyEstLatency)")
    assert(maxLatency <= (specialKeyEstLatency * (1 + acceptedError)), "Special key latency was " +
      s"much higher than expected: $maxLatency > $specialKeyEstLatency)")
    
    // https://damieng.com/blog/2014/12/11/sequence-averages-in-scala + comment
    // this is basically the running average algorithm (moving average)
    val nonMaxLatencyAvg = perfRecords.map(_._2._2)
                           .filterNot(_ == maxLatency)
                           .foldLeft((0.0, 1L)) {
                             case ((avg, count), nextLatency) =>
                               (avg + (nextLatency - avg)/ count, count + 1)
                           }._1
    // not implemented
    println("All assertion tests passed!")
  }
  
  private def buildBaseRDD(numPartitions: Int, partitionSize: Int) = {
    val data: Seq[String] = generateDataSeq(numPartitions, partitionSize, specialKey,
                                            otherKeyPrefix)
    println("Data: " + data)
    // this program heavily relies on distribution of data!
    val baseRdd = tempHadoopRDD("manyOfKey1", data,
                                minPartitions = Some(numPartitions),
                                requireExactPartitionCount = true)
    baseRdd
  }
  
  var _hasSleptForAggStats = false // agg stats are finalized via threadpool, so doing them
  // immediately is risky. Sleep a bit first to ensure that the results have been uploaded.
  private def printAggStats(rddId: Int, numPartitions: Int): Unit = {
    if(!_hasSleptForAggStats) {
      Thread.sleep(1000)
      _hasSleptForAggStats = true
    }
    val statsMap: Map[Int, AggregateLatencyStats] = getAggStats(rddId, numPartitions)
    val sep = "-" * 10 + s"$rddId (parts printed: $numPartitions)" + "-" * 10
    println(sep)
    statsMap.toSeq.sortBy(_._1).foreach {
      case (partition, stats) => println(s"$partition: $stats")
    }
    println(sep)
  }
  
  private def getAggStats(rddId: Int, numPartitions: Int): Map[Int, AggregateLatencyStats] = {
    AggregateStatsStorage.getInstance().getAllAggStats(appId, rddId, numPartitions)
  }
  
  /** Generate partitions of data and return as a single seq for hadoop writing. The first
   * partition consists solely of a unique key, which also appears at least once in all other
   * partitions. All other records are distinct/non-duplicated across partitions. For example:
   * [(foo, foo, foo), (foo, bar0, bar2), (foo, bar1, bar3)]
   */
  private def generateDataSeq(numPartitions: Int,
                              partitionSize: Int,
                              specialKey: String,
                              otherPrefix: String): Seq[String] = {
    def generatePartition(partition: Int): Seq[String] = {
      if(partition == 0) {
        Seq.fill(partitionSize)(specialKey)
      } else {
        Seq.tabulate(partitionSize)(idx => {
          // essentially the same as zipWithUniqueId, using numPartitions-1 because the first
          // partition is reserved
          if(idx == 0) specialKey else s"${otherPrefix}_${(numPartitions-1) * idx + partition}"
        })
      }
    }
    Seq.tabulate(numPartitions)(generatePartition).reduce(_ ++ _)
  }
}
