package org.apache.spark.lineage.perfdebug.ignite.lineageV2

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache, IgniteDataStreamer, Ignition}
import org.apache.spark.lineage.perfdebug.ignite.conf.IgniteManager
import org.apache.spark.lineage.perfdebug.lineageV2.CacheArguments
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.PartitionWithRecId

// Not quite an actual factory pattern, but useful for instantiating different KV cache types
object IgniteCacheFactory {
  // TODO figure out how to distribute this ignition across tasks
  // jteoh IGNITEMARKER
  //Ignition.setClientMode(true)
  //val ignite: Ignite = Ignition.ignite()
  // jteoh
  lazy val ignite: Ignite = IgniteManager.getIgniteInstance()
  def createIgniteCache[K, V](cacheArguments: CacheArguments): IgniteCache[K, V] = {
    val cacheConf = new CacheConfiguration[K, V](cacheArguments.cacheName)
      .setAffinity(
        new RendezvousAffinityFunction(false, cacheArguments.numPartitionsPerCache)
      )
    
    // Split statements for ease of debugging and clarity with getOrCreateCache
    val cache: IgniteCache[K, V] = ignite.getOrCreateCache(cacheConf)
    cache
  }
  
  def createIgniteCacheWithPRKey[V](
                                    cacheArguments: CacheArguments,
                                    keepPartitions: Boolean = false
                                   ): IgniteCache[PartitionWithRecId, V] = {
    val cacheConf = new CacheConfiguration[PartitionWithRecId, V](cacheArguments.cacheName)
      .setAffinity(
        new RendezvousAffinityFunction(false, cacheArguments.numPartitionsPerCache)
      )
      if(keepPartitions) {
        // Possible optimization: set affinity key such that joins within the same partition (e.g
        // . TapPreShuffle with it's predecessor TapHadoop) can be computed using zipPartitions
        // and iterator-based join, rather than a full shuffle. Doing this would also mean we
        // don't need to explicitly store partition id. However, it may be less clear from an
        // external query standpoint.
        /*cacheConf.setAffinityMapper(new AffinityKeyMapper {
          override def affinityKey(key: scala.Any): AnyRef = ???
    
          override def reset(): Unit = ???
        })*/
        new RendezvousAffinityFunction(false, cacheArguments.numPartitionsPerCache) {
          override def partition(key: scala.Any): Int = {
            key.asInstanceOf[PartitionWithRecId].partition
          }
        }
        // currently unused - still need to look more into how Ignite handles partitions between
        // different caches. Also, forcing the same partitioning as the original data could lead
        // to the same performance issues that the user application is facing.
      }
    // Split statements for ease of debugging and clarity with getOrCreateCache
    val cache: IgniteCache[PartitionWithRecId, V] = ignite.getOrCreateCache(cacheConf)
    cache
  }
  
  def createIgniteDataStreamer[V](
                                     cacheArguments: CacheArguments,
                                     keepPartitions: Boolean = false
                                 ): IgniteDataStreamer[PartitionWithRecId, V] = {
    // Create the cache if it doesn't already exist.
    val cache = createIgniteCacheWithPRKey(cacheArguments, keepPartitions)
    ignite.dataStreamer(cache.getName)
  }
}
