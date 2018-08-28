package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, PartitionWithRecId}
import org.apache.spark.rdd.RDD

case class PerfLineageCache(rdd: RDD[(PartitionWithRecId, (CacheValue, Long))]) {
  
  def lineageCache = LineageCache(rdd.mapValues(_._1))
  def latencies: RDD[Long] = rdd.map(PerfLineageCache.latencyExtractor)
  
  
}

object PerfLineageCache {
  def toPerfLineageCache[V<:CacheValue](rdd: RDD[(PartitionWithRecId, (V, Long))]
                                       ): PerfLineageCache =
    PerfLineageCache(rdd.asInstanceOf[RDD[(PartitionWithRecId, (CacheValue, Long))]])
  
  def latencyExtractor(r: (PartitionWithRecId, (CacheValue, Long))): Long = r._2._2
  
  def latencyOrdering: Ordering[(PartitionWithRecId,(CacheValue, Long))] = Ordering.by(latencyExtractor)
  
  
  implicit def perfLinCacheToRdd(lc: PerfLineageCache): RDD[(PartitionWithRecId,(CacheValue,
    Long))] = lc.rdd
  implicit def rddToPerfLinCache(rdd: RDD[(PartitionWithRecId,(CacheValue,Long))]): PerfLineageCache =
    PerfLineageCache(rdd)
}