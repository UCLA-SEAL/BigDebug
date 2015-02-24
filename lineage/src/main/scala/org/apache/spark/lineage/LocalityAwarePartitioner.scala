package org.apache.spark.lineage

import org.apache.spark.Partitioner
import org.apache.spark.lineage.LineageContext._

class LocalityAwarePartitioner(partitions: Int) extends Partitioner
{
  def numPartitions = partitions

  def getPartition(key: Any): Int = key.asInstanceOf[RecordId]._2.toInt

  override def equals(other: Any): Boolean = other match {
    case h: LocalityAwarePartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
