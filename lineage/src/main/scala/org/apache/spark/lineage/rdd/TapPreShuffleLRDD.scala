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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.spark.Dependency
import org.apache.spark.lineage.LineageContext
import org.apache.spark.util.PackIntIntoLong
import org.roaringbitmap.RoaringBitmap

import scala.reflect.ClassTag

private[spark]
class TapPreShuffleLRDD[T <: Product2[_, _]: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  @transient private var inputIdStore: Int2ObjectOpenHashMap[RoaringBitmap] = null

  override def getCachedData = shuffledData.setIsPreShuffleCache()

  override def materializeRecordInfo: Array[Any] = {
    inputIdStore.keySet().toIntArray.map(k =>
      new Tuple2(
        new Tuple2(
          PackIntIntoLong(tContext.stageId, splitId), k)
        , inputIdStore.get(k))
    ).toArray
  }

  override def initializeStores() = inputIdStore = new Int2ObjectOpenHashMap[RoaringBitmap](2097152)

  override def tap(record: T) = {
    val key = record._1.hashCode()
    val old = inputIdStore.get(key)
    if(old == null) {
      inputIdStore.put(key, RoaringBitmap.bitmapOf(tContext.currentInputId))
    } else {
      inputIdStore.put(key, {old.add(tContext.currentInputId); old})
    }

    record
  }

  private def update(value: Int) = (hadValue: Boolean, oldValue: RoaringBitmap) => {
    if (hadValue) {oldValue.add(value);oldValue} else RoaringBitmap.bitmapOf(value)
  }
}
