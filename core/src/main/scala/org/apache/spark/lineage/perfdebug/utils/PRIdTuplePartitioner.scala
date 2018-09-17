package org.apache.spark.lineage.perfdebug.utils

import org.apache.spark.Partitioner
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.PartitionWithRecId

/** Extremely simple partitioner that assumes the key a tuple, where the first element is an
 * instance of [[PartitionWithRecId]]. */
class PRIdTuplePartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions
  
  override def getPartition(key: Any): Int =
    key.asInstanceOf[(PartitionWithRecId, _)]._1.partition
  
  override def equals(other: Any): Boolean = other match {
    case p: PRIdTuplePartitioner =>
      p.numPartitions == numPartitions
    case p: PartitionWithRecIdPartitioner =>
      p.numPartitions == numPartitions
    case _ =>
      false
  }
}
