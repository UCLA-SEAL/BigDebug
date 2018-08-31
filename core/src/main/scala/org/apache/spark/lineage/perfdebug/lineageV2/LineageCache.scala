package org.apache.spark.lineage.perfdebug.lineageV2

import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, PartitionWithRecId}
import org.apache.spark.rdd.RDD

case class LineageCache(rdd: RDD[(PartitionWithRecId, CacheValue)]) {
  // This is a dangerous operation and should be used with care!
  def withValueType[V <: CacheValue]: RDD[(PartitionWithRecId, V)] =
    rdd.asInstanceOf[RDD[(PartitionWithRecId, V)]]
  
  def values: RDD[CacheValue] = rdd.values
  
  def inputIds: RDD[PartitionWithRecId] = {
    this.values.flatMap(_.inputIds).distinct() // distinct to avoid duplicates
  }
}
// In practice, the class instance is just another RDD.
object LineageCache {
  //  type LineageCache = RDD[(PartitionWithRecId, CacheValue)]
  implicit def linCacheToRdd(lc: LineageCache): RDD[(PartitionWithRecId,CacheValue)] = lc.rdd
  implicit def rddToLinCache(rdd: RDD[(PartitionWithRecId,CacheValue)]): LineageCache = LineageCache(rdd)
}

