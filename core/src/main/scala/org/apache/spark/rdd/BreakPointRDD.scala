package org.apache.spark.rdd

import org.apache.spark.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * Created by ali on 7/9/15.
 */

private[spark] class BreakPointRDD[T: ClassTag](
                                                 prev: RDD[T])
  extends RDD [T](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override val partitioner = prev.partitioner // Since filter cannot change a partition's keys

  override def compute(split: Partition, context: TaskContext) = {


    // firstParent[T].iterator(split, context).filter(f)
     firstParent[T].iterator(split, context)
  }
}