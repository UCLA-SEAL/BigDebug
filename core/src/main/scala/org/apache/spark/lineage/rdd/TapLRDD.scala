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
    val timeTaken = slowestPathHelper(dependencies)
    logInfo(s"time taken from paths: $timeTaken")
    buffer.put(PackIntIntoLong(splitId, id),  tContext.currentInputId, timeTaken)
    if(isLast) {
      (record, PackIntIntoLong(splitId, id)).asInstanceOf[T]
    } else {
      record
    }
  }
  
  def slowestPath(rdd: RDD[_]) : Long = {
      tContext.getRddRecordOutputTime(rdd.id) + slowestPathHelper(rdd.dependencies)
  }
  
  private def slowestPathHelper(deps: Seq[Dependency[_]]): Long = {
    if (deps.isEmpty) {
      0
    } else {
      deps.map(dep => slowestPath(dep.rdd)).max
    }
  }
}