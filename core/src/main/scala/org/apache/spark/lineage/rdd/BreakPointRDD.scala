package org.apache.spark.lineage.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect._

/**
 * Created by ali on 7/9/15.
 */

private[spark] class BreakPointRDD[T: ClassTag](prev: Lineage[T])
     extends RDD[T](prev)  with Lineage[T] {

     override val partitioner = prev.partitioner // Since filter cannot change a partition's keys

     override def ttag = classTag[T]

     override def getPartitions: Array[Partition] = firstParent[T].partitions

     override def lineageContext = prev.lineageContext

     override def compute(split: Partition, context: TaskContext) = {
          firstParent[T].iterator(split, context)
     }
}