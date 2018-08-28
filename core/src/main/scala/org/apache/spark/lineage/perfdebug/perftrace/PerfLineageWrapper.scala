package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.lineageV2.{LineageCacheDependencies, LineageWrapper}
import org.apache.spark.lineage.perfdebug.perftrace.PerfLineageCache.{latencyExtractor, latencyOrdering}
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, PartitionWithRecId}
import org.apache.spark.rdd.RDD

class PerfLineageWrapper(private val lineageDependencies: LineageCacheDependencies,
                         val perfCache: PerfLineageCache)
  extends LineageWrapper(lineageDependencies, perfCache.lineageCache) {
  
  /** Apply a filter/boolean function by latency */
  def filterLatency(fn: Long => Boolean): PerfLineageWrapper = {
    val newCache = perfCache.filter(r => fn(latencyExtractor(r)))
    this.withNewCache(newCache)
  }
  
  def latencies = perfCache.latencies
  
  def count: Long = perfCache.count()
  
  def percentile(percent: Double, ascending: Boolean = false): PerfLineageWrapper = {
    assert(percent > 0 && percent <= 1)
    val count = this.count
    val num = Math.floor(percent * count).toInt
    this.take(num, ascending=ascending)
  }
  
  /** Warning: don't use this with large numbers! */
  def take(num: Int, ascending: Boolean = false): PerfLineageWrapper = {
    // impl note: you could also do a sortBy followed by zipWithIndex and filter to preserve the
    // RDD abstraction. However, this also results in a full sort of the data, whereas I assume
    // top/takeOrdered are more efficiently implemented (eg with heaps)
    val topN: Array[(PartitionWithRecId, (CacheValue, Long))] = if (ascending) {
      perfCache.takeOrdered(num)(latencyOrdering)
    } else {
      perfCache.top(num)(latencyOrdering)
    }
    // ugly hack - might want to stick to using the lineage context but that's not easily
    // accessible.
    this.withNewCache(perfCache.context.parallelize(topN))
  }
  
  def takeDeprecated(num: Int, ascending: Boolean = false): PerfLineageWrapper = {
    // impl note: this is actually quite inefficient due to the sortBy, but is slightly cleaner
    // to write in code. In practice, it might be more efficient to take the top N and
    // parallelize that.
    val indexedSorted: RDD[(Long, (PartitionWithRecId, (CacheValue, Long)))] =
    perfCache.sortBy(latencyExtractor, ascending = ascending)
    .zipWithIndex()
    .map(_.swap)
    val newCache = indexedSorted.filter(_._1 < num).values
    this.withNewCache(newCache)
  }
  
  
  private def withNewCache(newCache: PerfLineageCache): PerfLineageWrapper = {
    PerfLineageWrapper(lineageDependencies, newCache)
  }
  
  //  /** Warning
  /**def percentile(percentile: Int, topPercent: Boolean = true): Unit = {
  
  }*/
}

object PerfLineageWrapper {
  def apply(lineageDependencies: LineageCacheDependencies,
            perfCache: PerfLineageCache): PerfLineageWrapper=
    new PerfLineageWrapper(lineageDependencies, perfCache)
}