package org.apache.spark.lineage.perfdebug.storage

// TODO number of cache partitions is currently fixed because the default 1024
// cannot be overridden globally or changed after creation, but is too high for local
// development. Using IgniteRDDs will result in one RDD partition per cache
// partition, and simple operations will end up spawning 1024 tasks.
case class CacheArguments(cacheName: String, numPartitionsPerCache: Int = 2)
