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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark._
import org.apache.spark.lineage.{LCacheManager, LineageContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.reflect._

private[spark]
class TapLRDD[T: ClassTag](@transient lc: LineageContext, @transient deps: Seq[Dependency[_]])
    extends RDD[T](lc.sparkContext, deps) with Lineage[T] {

  setCaptureLineage(true)

  @transient private[spark] var splitId: Int = 0

  @transient private[spark] var tContext: TaskContext = null

  @transient private[spark] var recordId: (Int, Int, Long) = (0, 0, 0L)

  // TODO make recordInfo grow in memory and spill to disk if needed
  private[spark] val recordInfo: ArrayBuffer[(Any, Any)] = new ArrayBuffer[(Any, Any)]()

  private[spark] var nextRecord: AtomicLong = new AtomicLong(0)

  private[spark] var shuffledData: ShuffledLRDD[_, _, _] = null

  private[spark] def newRecordId = nextRecord.getAndIncrement

  private[spark] def addRecordInfo(key: (Int, Int, Long), value: Seq[(_)]) = {
    value.foreach(v => recordInfo += key -> v)
  }

  /**
   * Compute an RDD partition or read it from a checkpoint if the RDD was checkpointed.
   */
  private[spark] override def computeOrReadCheckpoint(
     split: Partition,
     context: TaskContext): Iterator[T] = compute(split, context)

  override def ttag = classTag[T]

  override def lineageContext: LineageContext = lc

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def getRecordInfos = recordInfo

  override def compute(split: Partition, context: TaskContext) = {
    if(tContext == null) {
      tContext = context
    }
    splitId = split.index

    SparkEnv.get.cacheManager.asInstanceOf[LCacheManager].initMaterialization(this, split, StorageLevel.MEMORY_ONLY)

    firstParent[T].iterator(split, context).map(tap)
  }

  override def filter(f: T => Boolean): Lineage[T] = {
    new FilteredLRDD[T](this, sparkContext.clean(f))
  }

  def setCached(cache: ShuffledLRDD[_, _, _]): TapLRDD[T] = {
    shuffledData = cache
    this
  }

  def getCachedData = shuffledData.setIsPostShuffleCache()

  def tap(record: T): T = {
    recordId = (id, splitId, newRecordId)
    addRecordInfo(recordId, tContext.currentRecordInfo)

    record
  }
}