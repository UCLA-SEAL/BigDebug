package org.apache.spark.lineage.iterative

import org.apache.spark.lineage.{LineageContext, LineageManager}
import org.apache.spark.{Dependency, Partition, TaskContext, TaskContextImpl}
import org.apache.spark.lineage.rdd.{Lineage, MapPartitionsLRDD, TapLRDD}
import org.apache.spark.lineage.util.LongIntByteBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.util.PackIntIntoLong

import scala.reflect._

// Very similar to TapLRDD, but not strictly set to the end of a stage.
class IterationStartRDD[T: ClassTag](val iterationInput: Lineage[T])
  extends RDD[T](iterationInput) with Lineage[T] {
  this.setName("Iteration Start")
  this.cache()
  
  @transient private var tContext: TaskContextImpl = _
  @transient private var splitId: Short = _
  @transient private var nextRecord: Int = _
  
  private[spark] def newRecordId() = {
    nextRecord += 1
    nextRecord
  }
  
  private def myName = s"${this.getClass.getSimpleName}"
  
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    firstParent[T].iterator(split, context)
  }
  
  
  /** IterationStartRDD is never checkpointed - just recompute it. */
  override def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
    compute(split, context)
  
  /** IterationStartRDD is never cached because it's basically an identity function. The key
   * difference is that we *always* want to regenerate IDs. */
  override def getOrCompute(partition: Partition, context: TaskContext): Iterator[T] = {
    tContext = context.asInstanceOf[TaskContextImpl]
    splitId = partition.index.toShort
    nextRecord = -1
    super.getOrCompute(partition, context).map(record => {
      // generate a new record ID, under the assumption this iterator is deterministic.
      // otherwise, this is just an identity function (see #compute())
      tContext.currentInputId = newRecordId()
      record
    })
  }
  
  def lineageBuffer(): IterationStartLineageRDD[T] = {
    new IterationStartLineageRDD(this)
  }
  
  // assumption: this will be called before iterations actually start...
  def getPreIterationLineage(): Lineage[_] = {
    //lineageContext.setCaptureLineage(true)
    iterationInput.count() // trigger job
    iterationInput.getLineage()
  }
  
  override protected def getPartitions: Array[Partition] = firstParent[T].partitions
  
  override def ttag: ClassTag[T] = classTag[T]
  
  override def lineageContext = iterationInput.lineageContext
}
