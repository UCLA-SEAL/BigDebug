package org.apache.spark.lineage.ignite

import org.apache.spark.util.PackIntIntoLong
import org.apache.spark.util.collection.CompactBuffer

import scala.language.implicitConversions

object CacheDataTypes {
  /** PartitionWithRecId is a common format used throughout Titian - I've simply created a
    * wrapper to abstract out the details
    */
  case class PartitionWithRecId(value: Long) {
    def partition = PackIntIntoLong.getLeft(value)
    def split = partition // alias
    def recordId = PackIntIntoLong.getRight(value)
    def asTuple = PackIntIntoLong.extractToTuple(value)
  }
  /*case class PartitionWithRecId(partition: Int, recordId: Int) {
    def split = partition // alias
  }
  
  object PartitionWithRecId {
    def apply(value: Long): PartitionWithRecId = {
      val tuple: (Int, Int) = PackIntIntoLong.extractToTuple(value)
      PartitionWithRecId(tuple._1, tuple._2)
    }
  }
  */
  
  /** Temp base class for easy interpretation of record key in ignite. In practice we might not
   * want to actually store the key inside the value object, so this is subject to change/removal.
   */
  abstract class PerfIgniteCacheValue {
    def key: PartitionWithRecId
  }
  
  case class TapLRDDValue(outputId: PartitionWithRecId,
                          inputId: PartitionWithRecId,
                          latency: Long)
    extends PerfIgniteCacheValue {
    
    // TODO: integrate key type into API somewhere (IgniteCacheFactory?)
    def key: PartitionWithRecId = outputId
  }
  
  // Note (jteoh): inputIds is int[] because the split is always the same as the split found in
  // outputId. (Not sure why TapLRDD decided to specify long, but that's not the case here)
  case class TapPreShuffleLRDDValue(outputId: PartitionWithRecId,
                                    inputIds: Array[Int])
    extends PerfIgniteCacheValue {
    
    // TODO: integrate key type into API somewhere (IgniteCacheFactory?)
    def key: PartitionWithRecId = outputId
  
  }
  
  // TODO: figure out meaning of data types
  case class TapPostShuffleLRDDValue(outputId: PartitionWithRecId,
                                     compactBuffer: CompactBuffer[Long],
                                     bufferKey: Int)
    extends PerfIgniteCacheValue {
    
    def key: PartitionWithRecId = outputId
  
  }
  
  implicit def longToPartitionRec(value: Long): PartitionWithRecId = PartitionWithRecId(value)
}
