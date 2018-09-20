package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.lineageV2.LineageWrapper
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, PartitionWithRecId}
import org.apache.spark.rdd.RDD

trait PerfLineageWrapper extends LineageWrapper {
  this: LineageWrapper => // force implementations to also extend LineageWrapper
  
  /** Apply a filter/boolean function by latency */
  def filterLatency(fn: Long => Boolean): PerfLineageWrapper
  
  def latencies: RDD[Long]
  
  def count(): Long
  
  def percentile(percent: Double, ascending: Boolean = false): PerfLineageWrapper = {
    assert(percent > 0 && percent <= 1)
    val count = this.count
    val num = Math.floor(percent * count).toInt
    this.take(num, ascending=ascending)
  }
  
  /**
   * Takes the provided number of records, sorted by latency.
   * Warning: don't use this with large numbers!
   */
  def take(num: Int, ascending: Boolean = false): PerfLineageWrapper
  
  /** Legacy method to give an RDD suitable for debugging. Not to be used for actual computation! */
  def dataRdd: RDD[(PartitionWithRecId, (CacheValue, Long))]
}
