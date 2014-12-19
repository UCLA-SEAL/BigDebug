package org.apache.spark.lineage

import org.apache.spark.Partitioner

class LocalityAwarePartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions

  def getPartition(key: Any): Int = key.asInstanceOf[(Int, Int, Long)]._2

  override def equals(other: Any): Boolean = other match {
    case h: LocalityAwarePartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
