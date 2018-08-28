package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, PartitionWithRecId}
import org.apache.spark.rdd.RDD

case class LineageCache(rdd: RDD[(PartitionWithRecId, CacheValue)]) {
  // This is a dangerous operation and should be used with care!
  def withValueType[V <: CacheValue]: RDD[(PartitionWithRecId, V)] =
    rdd.asInstanceOf[RDD[(PartitionWithRecId, V)]]
  
  def values: RDD[CacheValue] = rdd.values
  
  def inputIds: RDD[PartitionWithRecId] = {
    this.values.flatMap(_.inputKeys).distinct() // distinct to avoid duplicates
  }
  
  private def lineageJoin(other: LineageCache): RDD[(PartitionWithRecId, (PartitionWithRecId, CacheValue))] = {
    // TODO potential performance optimization of lineage join
    // Requirements
    // 1. Ignite caches use affinity function that appropriately reflects partition ID.
    // 2. We can leverage knowledge of what's being tapped to determine if we're crossing a
    // shuffle boundary.
    // Result: in cases when we are crossing a shuffle boundary, we can call repartition ahead of
    // time to shuffle the data appropriately. Afterwards, we can leverage a map-side in-mem join
    // for better performance.
    inputIds.keyBy(identity).join(other.rdd)
  }
  
  // Join the two RDDs and return only records in `other`
  def lineageJoinToRight(other: LineageCache): LineageCache =
    lineageJoin(other).values
  // underlying assumption: the left-side partition id is the same as the key.
  // More correctly, we should return key + second value element in tuple.
  
}
// In practice, the class instance is just another RDD.
object LineageCache {
  //  type LineageCache = RDD[(PartitionWithRecId, CacheValue)]
  implicit def linCacheToRdd(lc: LineageCache): RDD[(PartitionWithRecId,CacheValue)] = lc.rdd
  implicit def rddToLinCache(rdd: RDD[(PartitionWithRecId,CacheValue)]): LineageCache = LineageCache(rdd)
}

