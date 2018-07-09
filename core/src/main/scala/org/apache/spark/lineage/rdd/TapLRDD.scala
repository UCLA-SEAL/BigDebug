/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.lineage.rdd

import org.apache.spark._
import org.apache.spark.lineage.util.LongIntLongByteBuffer
import org.apache.spark.lineage.{LineageContext, LineageManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.PackIntIntoLong

import scala.collection.mutable
import scala.reflect._

private[spark]
class TapLRDD[T: ClassTag](@transient lc: LineageContext, @transient deps: Seq[Dependency[_]])
    extends RDD[T](lc.sparkContext, deps) with Lineage[T] {

  @transient private[spark] var splitId: Short = 0

  @transient private[spark] var tContext: TaskContextImpl = _

  @transient private[spark] var nextRecord: Int = _

  @transient private var buffer: LongIntLongByteBuffer = _

  private var combine: Boolean = true

  tapRDD = Some(this)

  var isLast = false

  private[spark] var shuffledData: Lineage[_] = _

  setCaptureLineage(true)

  private[spark] def newRecordId() = {
    nextRecord += 1
    nextRecord
  }

  private[spark] override def computeOrReadCheckpoint(
     split: Partition,
     context: TaskContext): Iterator[T] = compute(split, context)

  override def ttag = classTag[T]

  override def lineageContext: LineageContext = lc

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) = {
    if(tContext == null) {
      tContext = context.asInstanceOf[TaskContextImpl]
    }
    splitId = split.index.toShort
    nextRecord = -1

    initializeBuffer()

    LineageManager.initMaterialization(this, split, context)

    // Make sure to time the current tap function here too.
    firstParent[T].iterator(split, context).map(measureTime(context, tap, this.id))
  }

  override def filter(f: T => Boolean): Lineage[T] = {
    val cleanF = sparkContext.clean(f)
    // Jason: need to understand this better
    // difference from Lineage#filter is that no "withScope" is used.
    new MapPartitionsLRDD[T, T](
      this, (context, pid, iter, rddId) => iter.filter(cleanF), preservesPartitioning = true)
  }

  override def materializeBuffer: Array[Any] = buffer.iterator.toArray.map(r => (r._1, r
    ._2.toLong, r._3))

  override def releaseBuffer(): Unit = {
    buffer.clear()
    tContext.addToBufferPool(buffer.getData)
  }

  def setCached(cache: Lineage[_]): TapLRDD[T] = {
    shuffledData = cache
    this
  }

  def combinerEnabled(enabled: Boolean) = {
    combine = enabled
    this
  }

  def isCombinerEnabled = combine

  def getCachedData = shuffledData.setIsPostShuffleCache()

  def initializeBuffer() = buffer = new LongIntLongByteBuffer(tContext.getFromBufferPool())

  def tap(record: T) = {
    val id = newRecordId()
    val timeTaken = computeSimpleTime()
    // logInfo(s"computed time: $timeTaken")
    buffer.put(PackIntIntoLong(splitId, id),  tContext.currentInputId, timeTaken)
    if(isLast) {
      (record, PackIntIntoLong(splitId, id)).asInstanceOf[T]
    } else {
      record
    }
  }
  
  // Simplified version of computeTotalTime. Should be the same in practice because task contexts
  // are only updated within the current stage.
  // 7/9/18 - Jason
  def computeSimpleTime(): Long = {
    tContext.getSummedRddRecordTime()
  }
  // Note on 6/21/18 (weeks after implementation below): This seems severely overcomplicated.
  // In practice, TapLRDD should only appear after one-to-one dependencies. A shuffle dependency
  // would trigger other subclasses, i.e. pre/post shuffle which should have their own
  // implementation.
  // In other words, there is only a limited subset of the code/possible execution paths that is
  // being exercised.
  // TODO Jason - optimize this further, eg if you can cache accumulated times within each RDD?
  // Do we want to be computing this now?
  // iterative implementation of a post-order DAG traversal
  // in recursive terms, this would compute accumulate(currValue, aggregate(childrenValues))
  // Because the current RDD has no time (it's still computing), we only return the aggregate over
  // its dependencies
  // IMPLEMENTATION NOTE: dependencyRDDs needs to be generated at creation time because this
  // RDD's dependencies will be updated during getLineage (where navigation only occurs between
  // taps). If this is removed or altered, this RDD will need an alternate way to determine it's
  // actual dependencies (as opposed to the taps used for lineage navigation)
  private val dependencyRDDs = this.dependencies.map(_.rdd)
  private var postOrderDeps: Option[Seq[RDD[_]]] = None
  private val cachedTimes = mutable.HashMap[RDD[_], Long]()
  
  def computeTotalTime(accumulateFunction: (Long, Long) => Long = _+_,
                       aggregateFunction:Seq[Long]=>Long = _.foldLeft(0L)(math.max)) : Long = {
    if(postOrderDeps.isEmpty) {
      // compute a post-order iteration of dependencies which need to be measured
      val s = mutable.Stack[RDD[_]](dependencyRDDs:_*)
      val seen = new mutable.HashSet[RDD[_]]
      val postOrderStack = new mutable.Stack[RDD[_]]
      while(s.nonEmpty) {
        val currRdd = s.pop()
        postOrderStack.push(currRdd)
        seen.add(currRdd)
        // general idea: existing TapLRDD impls will already have computed time for their
        // predecessors (but not their own .map function, which is why they're still included here)
        // Unfortunately this approach does not allow the final TapLRDD to be timed, so consider
        // refactoring.
        currRdd match {
          case _: TapLRDD[_] => // do not add dependencies since they were already accounted for.
          case _ => s.pushAll(currRdd.dependencies.map(_.rdd).filter(!seen(_)))
        }
      }
      postOrderDeps = Some(postOrderStack.toSeq)
    }
    
    // Some dependencies may appear in more than one RDD
    cachedTimes.clear() // rather than reallocate
    for(curr <- postOrderDeps.get) {
      cachedTimes(curr) = curr match {
        case _: TapLRDD[_] =>
          // dependencies already accounted for
          tContext.getRddRecordOutputTime(curr.id)
        case _ =>
          accumulateFunction(
            tContext.getRddRecordOutputTime(curr.id),
            aggregateFunction(curr.dependencies.map(_.rdd).map(cachedTimes(_))))
      }
    }
    // No time taken for the current RDD because it's still running...
    aggregateFunction(dependencyRDDs.map(cachedTimes(_)))
  }
  
  def printMethodTime[R](tag: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    val timeTaken = t1 - t0
    logInfo(s"$tag: ${timeTaken / 1000000} ms")
    result
  }
}