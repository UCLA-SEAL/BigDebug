package org.apache.spark.lineage

import org.apache.spark.Partitioner
import org.apache.spark.util.Utils

class HashAwarePartitioner(partitions: Int) extends Partitioner {

  def numPartitions = partitions

  def getPartition(key: Any): Int = Utils.nonNegativeMod(key.asInstanceOf[Int], numPartitions)

  override def equals(other: Any): Boolean = other match {
    case h: HashAwarePartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
}
