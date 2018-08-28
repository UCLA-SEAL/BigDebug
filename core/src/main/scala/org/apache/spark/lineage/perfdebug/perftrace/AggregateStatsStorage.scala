package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.ignite.perftrace.IgniteCacheAggregateStatsStorage
import org.apache.spark.lineage.rdd.LatencyStatsTap

/**
 * Storage object for handling [[AggregateLatencyStats]]
 * collected at a partition level for shuffle-based tap RDDs.
 */
trait AggregateStatsStorage {
  /** Retrieves all partition stats for the given RDD, as a map with key = partition and value =
   * stats for that partition.
   */
  def getAllAggStatsForRDD(appId: String,
                           aggStatsRdd: LatencyStatsTap[_]
                          ): Map[Int, AggregateLatencyStats]
  /**
   * DEBUGGING ONLY Retrieves all partition->stats mapping for the provided RDD ID
   */
  def getAllAggStats(appId: String,
                     rddId: Int,
                     numPartitions: Int): Map[Int, AggregateLatencyStats]
  
  def saveAggStats(appId: String,
                   aggStatsRdd: LatencyStatsTap[_]): Unit
}

object AggregateStatsStorage {
  // TODO make this configurable via conf in the future. Also consider integrating with SparkEnv
  private var _instance: Option[AggregateStatsStorage] =
    Some(new IgniteCacheAggregateStatsStorage())
  def getInstance(): AggregateStatsStorage = {
    _instance.getOrElse(
      throw new IllegalStateException("No AggregateStatsRepo instance has been set. Did you " +
                                        "mean to call AggregateStatsRepo.setInstance(...)?")
    )
  }
  
  def setInstance(instance: AggregateStatsStorage) =
    _instance = Option(instance)
}
