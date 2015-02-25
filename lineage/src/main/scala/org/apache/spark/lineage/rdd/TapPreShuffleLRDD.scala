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

import org.apache.spark.lineage.{LCacheManager, LineageContext, PrimitiveKeyOpenHashMap}
import org.apache.spark.util.PackShortIntoInt
import org.apache.spark.{Dependency, Partition, SparkEnv, TaskContext, TaskContextImpl}
import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[spark]
class TapPreShuffleLRDD[T <: Product2[_, _]: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  override def getCachedData = shuffledData.setIsPreShuffleCache()

  @transient var tmp: PrimitiveKeyOpenHashMap[Int, Int] = null
  @transient var tmp2: ArrayBuffer[RoaringBitmap] = null

  override def materializeRecordInfo: Array[Any] = {
    tmp.iterator.map(r => new Tuple2(new Tuple2(PackShortIntoInt(tContext.stageId.toShort, splitId), r._1), r._2)).toArray
//    tmp.zipWithIndex.map(r => ((tContext.stageId.toShort, splitId, r._2), r._1._2)).toArray.asInstanceOf[Array[Any]]
//    tmp.zipWithIndex.flatMap(r => {
//      r._1._2.toArray.map(
//        r2 => ((tContext.stageId.toShort, splitId, r._2), r2))
//    }).toArray.asInstanceOf[Array[Any]]
  }

  override def compute(split: Partition, context: TaskContext) = {
    if(tContext == null) {
      tContext = context.asInstanceOf[TaskContextImpl]
    }
    splitId = split.index.toShort

    SparkEnv.get.cacheManager.asInstanceOf[LCacheManager].initMaterialization(this, split)

    tmp = new PrimitiveKeyOpenHashMap[Int, Int]
    tmp2 = new ArrayBuffer[RoaringBitmap](1)

    firstParent[T].iterator(split, context).map(tap)
  }

  private[spark] def update(value: Int) = (hadValue: Boolean, oldValue: RoaringBitmap) => {
    if (hadValue) {oldValue.add(value);oldValue} else RoaringBitmap.bitmapOf(value)
  }

  override def tap(record: T) = {
   // tmp.changeValue(record._1.hashCode(), update(tContext.currentRecordInfo))
    val index = tmp.update(record._1.hashCode()) -1
    if (tmp2.size == index) {
      tmp2.append(RoaringBitmap.bitmapOf(tContext.currentRecordInfo))
    } else {
      tmp2(index).add(tContext.currentRecordInfo)
    }
    record
  }
}
