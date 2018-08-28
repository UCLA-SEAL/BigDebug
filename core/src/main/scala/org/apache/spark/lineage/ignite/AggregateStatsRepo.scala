package org.apache.spark.lineage.ignite

import org.apache.spark.lineage.rdd.LatencyStatsTap

/** TODO: migrate to its own perf-lineage package separate from ignite dependencies.
 */
trait AggregateStatsRepo {
  /** Retrieves all partition stats for the given RDD, as a map with key = partition and value =
   * stats for that partition.
   */
  def getAllAggStatsForRDD(appId: String,
                           aggStatsRdd: LatencyStatsTap[_]
                          ): Map[Int, AggregateLatencyStats]
  
  
  def saveAggStats(appId: String,
                   aggStatsRdd: LatencyStatsTap[_]): Unit
}
