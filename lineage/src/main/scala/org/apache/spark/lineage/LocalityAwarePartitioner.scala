package org.apache.spark.lineage

import org.apache.spark.Partitioner
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.util.PackIntIntoLong

class LocalityAwarePartitioner(partitions: Int) extends Partitioner {

  def numPartitions = partitions

  def getPartition(key: Any): Int = PackIntIntoLong.getRight(key.asInstanceOf[RecordId]._1)

  override def equals(other: Any): Boolean = other match {
    case h: LocalityAwarePartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
