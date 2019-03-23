package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.Latency
import org.apache.spark.lineage.perfdebug.lineageV2.LineageCache
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, PartitionWithRecId}
import org.apache.spark.rdd.RDD

case class PerfLineageCache(rdd: RDD[(PartitionWithRecId, (CacheValue, Latency))]) {
  
  def lineageCache = LineageCache(rdd.mapValues(_._1))
  def latencies: RDD[Latency] = rdd.map(PerfLineageCache.latencyExtractor)
  
  
}

object PerfLineageCache {
  def toPerfLineageCache[V<:CacheValue](rdd: RDD[(PartitionWithRecId, (V, Latency))]
                                       ): PerfLineageCache =
    PerfLineageCache(rdd.asInstanceOf[RDD[(PartitionWithRecId, (CacheValue, Latency))]])
  
  def latencyExtractor(r: (PartitionWithRecId, (CacheValue, Latency))): Latency = r._2._2
  
  def latencyOrdering: Ordering[(PartitionWithRecId,(CacheValue, Latency))] = Ordering.by(latencyExtractor)
  
  
  implicit def perfLinCacheToRdd(lc: PerfLineageCache):
                  RDD[(PartitionWithRecId,(CacheValue, Latency))] = lc.rdd
  implicit def rddToPerfLinCache(rdd: RDD[(PartitionWithRecId,(CacheValue, Latency))]):
                  PerfLineageCache = PerfLineageCache(rdd)
}