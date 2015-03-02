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

import org.apache.spark.Dependency
import org.apache.spark.lineage.{LineageContext, PrimitiveKeyOpenHashMap}
import org.apache.spark.util.PackShortIntoInt
import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[spark]
class TapPreShuffleLRDD[T <: Product2[_, _]: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  @transient private var inputIdStore: PrimitiveKeyOpenHashMap[Int, Int] = null
  @transient private var inputIdStore2: ArrayBuffer[RoaringBitmap] = null

  override def getCachedData = shuffledData.setIsPreShuffleCache()

  override def materializeRecordInfo: Array[Any] = inputIdStore.iterator.map(r =>
      new Tuple2(new Tuple2(PackShortIntoInt(tContext.stageId, splitId), r._1), inputIdStore2(r._2 -1))).toArray

  override def initializeStores() = {
    inputIdStore = new PrimitiveKeyOpenHashMap
    inputIdStore2 = new ArrayBuffer(1)
  }

  override def tap(record: T) = {
    val index = inputIdStore.update(record._1.hashCode()) -1
    if (inputIdStore2.size == index) {
      inputIdStore2.append(RoaringBitmap.bitmapOf(tContext.currentInputId))
    } else {
      inputIdStore2(index).add(tContext.currentInputId)
    }
    record
  }

  private def update(value: Int) = (hadValue: Boolean, oldValue: RoaringBitmap) => {
    if (hadValue) {oldValue.add(value);oldValue} else RoaringBitmap.bitmapOf(value)
  }
}
