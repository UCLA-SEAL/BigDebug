package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.lineageV2.{LineageCacheDependencies, LineageWrapper}
import org.apache.spark.lineage.perfdebug.perftrace.PerfLineageCache.{latencyExtractor, latencyOrdering}
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, PartitionWithRecId}
import org.apache.spark.rdd.RDD

/**
 * Default implementation for [[PerfLineageWrapper]], which uses
 * a [[PerfLineageCache]] which is in turn a wrapper around an
 * RDD of (ID, (CacheValue, Latency)). This implementation is used with the
 * initial prototype of [[PerfTraceCalculator]], [[PerfTraceCalculatorV1]].
 * In practice, it might be more efficient to run latency-related operations
 * on just the ID+Latencys (ignoring the [[CacheValue]] objects, which may be large).
 * The downside to this approach is that the IDs would need to be joined back
 * to the original data to get the CacheValue instances (required for lineage),
 * but the record size during latency analysis would be much smaller. This
 * tradeoff is implemented in [[IdOnlyPerfLineageWrapper]].
 */
 
class DefaultPerfLineageWrapper(private val lineageDependencies: LineageCacheDependencies,
                                private val perfCache: PerfLineageCache)
  extends LineageWrapper(lineageDependencies, perfCache.lineageCache) with PerfLineageWrapper {
  
  /** Apply a filter/boolean function by latency */
  override def filterLatency(fn: Long => Boolean): DefaultPerfLineageWrapper = {
    val newCache = perfCache.filter(r => fn(latencyExtractor(r)))
    this.withNewCache(newCache)
  }
  
  override def latencies: RDD[Long] = perfCache.latencies
  
  override def count(): Long = perfCache.count()
  
  /**
   * Takes the provided number of records, sorted by latency.
   * Warning: don't use this with large numbers!
   */
  override def take(num: Int, ascending: Boolean = false): DefaultPerfLineageWrapper = {
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
  
  def takeDeprecated(num: Int, ascending: Boolean = false): DefaultPerfLineageWrapper = {
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
  
  override def dataRdd: RDD[(PartitionWithRecId, (CacheValue, Long))] = {
    perfCache.rdd
  }
  
  private def withNewCache(newCache: PerfLineageCache): DefaultPerfLineageWrapper = {
    DefaultPerfLineageWrapper(lineageDependencies, newCache)
  }
  
}

object DefaultPerfLineageWrapper {
  def apply(lineageDependencies: LineageCacheDependencies,
            perfCache: PerfLineageCache): DefaultPerfLineageWrapper=
    new DefaultPerfLineageWrapper(lineageDependencies, perfCache)
}