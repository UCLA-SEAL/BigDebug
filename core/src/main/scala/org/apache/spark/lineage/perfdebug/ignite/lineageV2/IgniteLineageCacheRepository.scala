package org.apache.spark.lineage.perfdebug.ignite.lineageV2

import org.apache.ignite.IgniteCache
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.lineage.perfdebug.lineageV2.{LineageCache, LineageCacheDependencies, LineageCacheRepository}
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, PartitionWithRecId}

class IgniteLineageCacheRepository(igniteContext: IgniteContext)
  extends LineageCacheRepository {
  override def getCache(name: String): LineageCache = {
    igniteContext.fromCache[PartitionWithRecId, CacheValue](name)
  }
  
  private val RESERVED_DEPENDENCIES_CACHE_NAME = "__PERF_IGNITE_DEPENDENCIES_CACHE"
  private val DEPENDENCIES_CACHE_PARTITION_COUNT = 1
  
  // Leverage the ignite cache API rather than RDD API because it's more straightforward here
  // TODO - limit size somehow
  private val DEPENDENCIES_CACHE: IgniteCache[String, LineageCacheDependencies] =
    igniteContext.ignite().getOrCreateCache(
      new CacheConfiguration[String, LineageCacheDependencies](RESERVED_DEPENDENCIES_CACHE_NAME)
        .setAffinity(new RendezvousAffinityFunction(false, DEPENDENCIES_CACHE_PARTITION_COUNT))
    )
  
  
  override def getCacheDependencies(jobId: String): LineageCacheDependencies = {
    val result = DEPENDENCIES_CACHE.get(jobId)
    if (result == null)
      throw new IllegalArgumentException(s"ID $jobId does not have any cache dependencies!")
    result
  }
  
  override def saveCacheDependencies(jobId: String,
                                     lineageCacheDependencies: LineageCacheDependencies): Unit = {
    DEPENDENCIES_CACHE.put(jobId, lineageCacheDependencies)
  }
  
  override def close(): Unit = igniteContext.close() // do we need to close on workers too?
}
