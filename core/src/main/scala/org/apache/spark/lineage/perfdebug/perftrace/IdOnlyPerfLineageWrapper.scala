package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.lineageV2.{LineageCacheDependencies, LineageWrapper}
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, PartitionWithRecId}
import org.apache.spark.rdd.RDD
import IdOnlyPerfLineageWrapper._
import org.apache.spark.Latency

/**
 * Implementation of [[PerfLineageWrapper]] that supports latency analysis on an RDD where
 * records are of type (ID, Latency). Notably, the [[CacheValue]]s for each ID are retained in a
 * separate reference RDD for later joining once lineage is required. The advantage to this
 * implementation is that record sizes are much smaller during latency, but the tradeoff is that
 * an additional join is required to actually computing lineage.
 *
 * This implementation is primarily used with [[PerfTraceCalculatorV2]]. Further discussion of
 * the tradeoffs may be found in [[DefaultPerfLineageWrapper]].
 */
class IdOnlyPerfLineageWrapper(
                               private val lineageDependencies: LineageCacheDependencies,
                               // the current list of IDs and latencies
                               private val idAndLatencyRDD: RDD[(PartitionWithRecId, Latency)],
                               // the original lineage RDD from the external cache. this
                               // can reflect the entire dataset, so you need to filter
                               // with idRDD!
                               private val baseRDD: RDD[(PartitionWithRecId, CacheValue)]
                               ) extends LineageWrapper(lineageDependencies,
                                                        filterJoin(idAndLatencyRDD.keys,
                                                                   baseRDD))
                                with PerfLineageWrapper {
  /** Apply a filter/boolean function by latency */
  override def filterLatency(fn: Latency => Boolean): PerfLineageWrapper =
    this.withNewIdAndLatencyRDD(idAndLatencyRDD.filter(r => fn(latencyExtractor(r))))
  
  override def latencies: RDD[Latency] = idAndLatencyRDD.values
  
  override def count(): Long = idAndLatencyRDD.count()
  
  // TODO: define a percentile function?
  // override def percentile(percent: Double, ascending: Boolean): PerfLineageWrapper = ???
  
  /**
   * Takes the provided number of records, sorted by latency.
   * Warning: don't use this with large numbers!
   */
  override def take(num: Int, ascending: Boolean): PerfLineageWrapper = {
    // impl note: you could also do a sortBy followed by zipWithIndex and filter to preserve the
    // RDD abstraction. However, this also results in a full sort of the data, whereas I assume
    // top/takeOrdered are more efficiently implemented (eg with heaps)
    val topN: Array[(PartitionWithRecId, Latency)] = if (ascending) {
      idAndLatencyRDD.takeOrdered(num)(latencyOrdering)
    } else {
      idAndLatencyRDD.top(num)(latencyOrdering)
    }
    // ugly hack - might want to stick to using the lineage context but that's not easily
    // accessible.
    this.withNewIdAndLatencyRDD(idAndLatencyRDD.context.parallelize(topN))
  }
  
  override def dataRdd: RDD[(PartitionWithRecId, (CacheValue, Latency))] = {
    baseRDD.join(idAndLatencyRDD)
  }
  
 private def withNewIdAndLatencyRDD(newRDD: RDD[(PartitionWithRecId, Latency)]
                                    ): IdOnlyPerfLineageWrapper = {
   IdOnlyPerfLineageWrapper(lineageDependencies, newRDD, baseRDD)
  }
}

object IdOnlyPerfLineageWrapper {
  def apply(lineageDependencies: LineageCacheDependencies,
            idAndLatencyRDD: RDD[(PartitionWithRecId, Latency)],
            baseRDD: RDD[(PartitionWithRecId, CacheValue)]
            ): IdOnlyPerfLineageWrapper = {
    new IdOnlyPerfLineageWrapper(lineageDependencies, idAndLatencyRDD, baseRDD)
  }
  
  def filterJoin(idRDD: RDD[PartitionWithRecId],
                 baseRDD: RDD[(PartitionWithRecId, CacheValue)]
                ): RDD[(PartitionWithRecId, CacheValue)] = {
    // filter the baseRDD by the IDs present in idRDD via a join.
    // TODO jteoh: determine if there's an optimization to be made with useShuffle?
    val join: RDD[(PartitionWithRecId, (PartitionWithRecId, CacheValue))] =
      LineageWrapper.joinLineageKeyedRDDs(idRDD.keyBy(identity), baseRDD, useShuffle = true)
    join.values
  }
  
  def latencyExtractor(r: (PartitionWithRecId, Latency)): Latency = r._2
  def latencyOrdering: Ordering[(PartitionWithRecId, Latency)] = Ordering.by(latencyExtractor)
}
