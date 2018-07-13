package org.apache.spark.lineage.ignite

import org.apache.ignite.cache.affinity.AffinityKeyMapper
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.spark.lineage.ignite.CacheDataTypes.PartitionWithRecId



// Not quite an actual factory pattern, but useful for instantiating different KV cache types
object IgniteCacheFactory {
  val ignite: Ignite = Ignition.ignite()

  def createIgniteCache[K, V](cacheArguments: CacheArguments): IgniteCache[K, V] = {
    val cacheConf = new CacheConfiguration[K, V](cacheArguments.cacheName)
      .setAffinity(
        new RendezvousAffinityFunction(false, cacheArguments.numPartitionsPerCache)
      )
    
    // Split statements for ease of debugging and clarity with getOrCreateCache
    val cache: IgniteCache[K, V] = ignite.getOrCreateCache(cacheConf)
    cache
  }
  
  def createIgniteCacheWithPRKey[V](cacheArguments: CacheArguments, keepPartitions: Boolean = false)
  : IgniteCache[PartitionWithRecId, V]
  = {
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
      }
    // Split statements for ease of debugging and clarity with getOrCreateCache
    val cache: IgniteCache[PartitionWithRecId, V] = ignite.getOrCreateCache(cacheConf)
    cache
  }
}

// TODO number of cache partitions is currently fixed because the default 1024
// cannot be overridden globally or changed after creation, but is too high for local
// development. Using IgniteRDDs will result in one RDD partition per cache
// partition, and simple operations will end up spawning 1024 tasks.
case class CacheArguments(cacheName: String, numPartitionsPerCache: Int = 2)
