package org.apache.spark.lineage.perfdebug.perfmetrics

import org.apache.ignite.{Ignite, IgniteCache}
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.spark.lineage.perfdebug.ignite.conf.IgniteManager
import scala.collection.JavaConverters._

class IgnitePerfMetricsStorage(val ignite: Ignite = {
                                //Ignition.setClientMode(true)
                                //Ignition.ignite() // jteoh IGNITEMARKER
                                IgniteManager.getIgniteInstance()
                              }) extends PerfMetricsStorage {
  // Design note: unlike AggStats equivalent, this will create one new cache per stage.
  // (AggStats's cache is per-app, with key = (rdd + partition) which made for some confusion
  // with getting all entries for a given RDD)
  
  private val CACHE_PARTITION_COUNT = 1
  private val RESERVED_PERF_METRICS_CACHE_PREFIX = "__PERF_METRICS_CACHE"
  /** Retrieves all partition stats for the given RDD, as a map with key = partition and value =
   * stats for that partition.
   */
  override def getPerfMetricsForStage(appId: String,
                                      jobId: JobId,
                                      stageId: StageId): Map[PartitionId, PerfMetricsStats] = {
    val cache = getCache(appId, jobId, stageId)
    // getCache returns an IgniteCache (javax Cache), so convert it to a map as needed.
    cache.iterator().asScala.map(entry => entry.getKey -> entry.getValue).toMap
  }
  
  override def savePerfMetrics(appId: String,
                               jobId: JobId,
                               stageId: StageId,
                               partitionId: PartitionId,
                               data: PerfMetricsStats): Unit = {
    val cache = getCache(appId, jobId, stageId)
    cache.put(partitionId, data)
  }
  
  private def getCacheName(appId: String, jobId: JobId, stageId: StageId): String =
    s"${RESERVED_PERF_METRICS_CACHE_PREFIX}_$appId-$jobId-$stageId"
  /** Clone of LineageManager's method to easily support schema changes during development.
   * TODO re-evaluate configurations for scaled usage if needed. This is generally a very
   * lightweight cache in comparison to other finer-grained metrics (eg individual record
   * latency) */
  private def getCache(appId: String, jobId: JobId, stageId: StageId)
  : IgniteCache[PartitionId, PerfMetricsStats] = {
    val cacheName = getCacheName(appId, jobId, stageId)
    val cacheConf = new CacheConfiguration[PartitionId, PerfMetricsStats](cacheName)
      .setAffinity(new RendezvousAffinityFunction(false, CACHE_PARTITION_COUNT))
    
    val cache: IgniteCache[PartitionId, PerfMetricsStats] = ignite.getOrCreateCache(cacheConf)
    cache
  }
}
