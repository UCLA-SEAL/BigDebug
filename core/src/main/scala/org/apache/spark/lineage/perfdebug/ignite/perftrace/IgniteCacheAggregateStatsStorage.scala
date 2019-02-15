package org.apache.spark.lineage.perfdebug.ignite.perftrace

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.spark.lineage.perfdebug.ignite.conf.IgniteManager
import org.apache.spark.lineage.perfdebug.perftrace.{AggregateLatencyStats, AggregateStatsStorage}
import org.apache.spark.lineage.rdd.LatencyStatsTap

import scala.collection.JavaConverters._

/**
 * This builds one cache per app. The cache is keyed by both RDD and partition ID. If all entries
 * for a given RDD are desired, one should leverage the number of partitions for that RDD (0 to
 * numPartitions - 1) or use the getAll* method provided.
 * In practice, this cache is expected to be fairly small, at least in comparison to the amount
 * of data. We expect one entry in the cache per RDD id + partition.
 */
class IgniteCacheAggregateStatsStorage(ignite: Ignite = {
                                        //Ignition.setClientMode(true)
                                        //Ignition.ignite() // jteoh IGNITEMARKER
                                        IgniteManager.getIgniteInstance()
                                      }) extends
  AggregateStatsStorage {
  private val RESERVED_AGG_STATS_CACHE_BASE_NAME = "__PERF_IGNITE_AGG_STATS_CACHE"
  private val CACHE_PARTITION_COUNT = 1
  
  // Originally the setup was (RDD_ID, Map[Partition), Stats]). However, this can lead to
  // synchronization issues as the value (map) has to be updated and re-inserted. Instead, we
  // maintain keys so that no two tasks will overwrite the same value. To retrieve all entries
  // for an RDD, iterate through the number of partitions it has.
  type RddId = Int
  type PartitionId = Int
  private type CacheKey = (RddId, PartitionId)
  private type CacheValue = String // storage format for ignite
  
  private val separator = ","
  // Experimental: Trying to store in a common/non-java format.
  private def serializeStats(stats: AggregateLatencyStats): CacheValue =
    AggregateLatencyStats.unapply(stats).get.productIterator.mkString(separator)
  
  private def deserializeStats(value: CacheValue): AggregateLatencyStats = {
    val split = value.split(separator, 3)
    AggregateLatencyStats(split(0).toLong, split(1).toLong, split(2).toLong)
  }
  
  private def buildAggStatsCacheName(appId: String) = {
    // TODO set up cache name properly
    s"${RESERVED_AGG_STATS_CACHE_BASE_NAME}_${appId}"
  }
  
  private def getCache(appId: String): IgniteCache[CacheKey, CacheValue] = {
    val cacheName = buildAggStatsCacheName(appId)
    ignite.getOrCreateCache(
      new CacheConfiguration[CacheKey, CacheValue](cacheName)
        .setAffinity(new RendezvousAffinityFunction(false, CACHE_PARTITION_COUNT))
    )
  }
  
  /** Get aggregate latency stats for a given RDD. K->V is Partition->Stats */
  def getAggStats(appId: String,
                  rddId: RddId, partition: PartitionId): AggregateLatencyStats = {
    val tuple = getCache(appId).get((rddId, partition))
    deserializeStats(tuple)
  }
  
  override def getAllAggStatsForRDD(appId: String,
                                    aggStatsRdd: LatencyStatsTap[_]
                                   ): Map[PartitionId, AggregateLatencyStats] = {
    getAllAggStats(appId, aggStatsRdd.id, aggStatsRdd.getNumPartitions)
  }
  
  override def getAllAggStats(appId: String,
                     rddId: RddId,
                     numPartitions: Int): Map[PartitionId, AggregateLatencyStats] = {
    val cache = getCache(appId)
    val keys = (0 until numPartitions).map {(rddId, _)}
    // Some fun java-scala conversions to match the ignite API. Also use toMap to ensure an
    // immutable map is returned at the end.
    val result = cache.getAll(keys.toSet.asJava).asScala.toMap.map {
      case (rddIdAndPartition, value) => (rddIdAndPartition._2, deserializeStats(value))
    }
    if (result.size != numPartitions) {
      println("AggStatsRepo WARN: agg partition count is not equal to expected partition count -" +
                s"${result.size} < $numPartitions")
    }
    result
  }
  
  override def saveAggStats(appId: String,
                            aggStatsRdd: LatencyStatsTap[_]): Unit = {
    saveAggStats(appId, aggStatsRdd.id, aggStatsRdd.splitId.toInt, aggStatsRdd.getLatencyStats)
  }
  
  private def saveAggStats(appId: String,
                           rddId: RddId,
                           partition: PartitionId,
                           stats: AggregateLatencyStats): Unit = {
    val cache = getCache(appId)
    val key = (rddId, partition)
    cache.put(key, serializeStats(stats))
  }
}
