package org.apache.spark.lineage.ignite

import org.apache.ignite.IgniteCache
import org.apache.spark.lineage
import org.apache.spark.lineage.ignite
import org.apache.spark.util.PackIntIntoLong

object CacheDataTypes {
  type OutputPartitionWithRecId = PartitionWithRecId
  type InputPartitionWithRecId = PartitionWithRecId
  type PartitionId = Int
  type OutputPartitionId = PartitionId
  type InputPartitionId = PartitionId
  type RecId = Int
  type OutputRecId = RecId
  type InputRecId = RecId
  type LatencyMs = Long
  
  type TapLRDDCache = IgniteCache[OutputPartitionWithRecId, (OutputPartitionWithRecId,
    InputPartitionWithRecId, LatencyMs)]
  
  /** PartitionWithRecId is a common format used throughout Titian - I've simply created a
 * wrapper to abstract out the details */
  case class PartitionWithRecId(value: Long) {
    def partition = PackIntIntoLong.getLeft(value)
    def split = partition // alias
    def recordId = PackIntIntoLong.getRight(value)
    def asTuple = PackIntIntoLong.extractToTuple(value)
  }
  
  case class TapLRDDValue(outputId: PartitionWithRecId, inputId: PartitionWithRecId,
                          latency: Long) {
//    def apply(output: Long, input: Long, latency: Long): TapLRDDValue =
//      TapLRDDValue(output, input, latency)
    def key: PartitionWithRecId = outputId
  }
  object TapLRDDValue {
    /** Expected materialized buffer type is encoded here */
    def apply(value: Any): TapLRDDValue = long3pleToTapLRDDValue(value.asInstanceOf[(Long, Long, Long)])
  }
  
  implicit def longToPartitionRec(value: Long): PartitionWithRecId = PartitionWithRecId(value)
  implicit def long3pleToTapLRDDValue(tuple3: (Long, Long, Long)): TapLRDDValue = {
    TapLRDDValue(tuple3._1, tuple3._2, tuple3._3)
  }
}
