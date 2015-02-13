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

import com.google.common.hash.Hashing
import org.apache.spark.lineage.{AppendOnlyMap, LCacheManager, LineageContext}
import org.apache.spark.{Dependency, Partition, SparkEnv, TaskContext}
import org.roaringbitmap.RoaringBitmap

import scala.reflect.ClassTag

private[spark]
class TapPreShuffleLRDD[T <: Product2[_, _]: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  override def getCachedData = shuffledData.setIsPreShuffleCache()

  //@transient var tmp: AppendOnlyMap[Any, Set[(Short, Int)]] = null
  //@transient private var tmp: ExternalAppendOnlyMap[Any, (Short, Int), Set[(Short, Int)]] = null
  //@transient private var tmp: ExternalAppendOnlyMap[Any, Int, Set[Int]] = null
  @transient var tmp: AppendOnlyMap[RoaringBitmap] = null
  //@transient private var tmp: ExternalAppendOnlyMap[Int, Int, RoaringBitmap] = null
  //@transient var tmp: PrimitiveKeyOpenHashMap[Int, RoaringBitmap] = null
  //@transient var tmp: TreeMultimap[Int, Int] = null

  override def materializeRecordInfo: Array[Any] = { Array()
 //   tmp.zipWithIndex.map(r => ((tContext.stageId.toShort, splitId, r._2), r._1._2)).toArray.asInstanceOf[Array[Any]]
//    tmp.zipWithIndex.flatMap(r => {
//      r._1._2.toArray.map(
//        r2 => ((tContext.stageId.toShort, splitId, r._2), r2))
//    }).toArray.asInstanceOf[Array[Any]]
  }

  override def compute(split: Partition, context: TaskContext) = {
    if(tContext == null) {
      tContext = context
    }
    splitId = split.index.toShort

    //recordInfo = new ArrayBuffer[(Any, Any)]()

    SparkEnv.get.cacheManager.asInstanceOf[LCacheManager].initMaterialization(this, split)

//    def mergeValues(c: Set[Int], e: Int):  Set[Int] = c + e

//    def mergeValues(c: RoaringBitmap, e: Int):  RoaringBitmap = {
//      c.add(e)
//      c
//    }

    //tmp = new ExternalAppendOnlyMap[Any , (Short, Int), Set[(Short, Int)]]((e: (Short, Int)) => Set(e), mergeValues, (c1: Set[(Short, Int)], c2: Set[(Short, Int)]) => c1 ++ c2)

    //tmp = new AppendOnlyMap[Any, Set[(Short, Int)]]()

    //tmp = new AppendOnlyMap[Int, EWAHCompressedBitmap]()

    //tmp = new ExternalAppendOnlyMap[Any , Int, Set[Int]]((e: Int) => Set(e), mergeValues, (c1: Set[Int], c2: Set[Int]) => c1 ++ c2)

    //tContext.currentRecordInfos = new ExternalAppendOnlyMap[Int, Int, EWAHCompressedBitmap]((e: Int) => EWAHCompressedBitmap.bitmapOf(e), mergeValues, (c1: EWAHCompressedBitmap, c2: EWAHCompressedBitmap) => c1.and(c2))

    //tmp = new ExternalAppendOnlyMap[Int, Int, RoaringBitmap]((e: Int) => RoaringBitmap.bitmapOf(e), mergeValues, (c1: RoaringBitmap, c2: RoaringBitmap) => RoaringBitmap.or(c1, c2))

    //tmp = new PrimitiveKeyOpenHashMap[Int, RoaringBitmap](60000000)

    //tmp = TreeMultimap.create()

    tmp = new AppendOnlyMap[RoaringBitmap]()


    firstParent[T].iterator(split, context).map(tap)
  }

  override def cleanTable = {
      tmp.clear
      tmp = null
//    if(tmp != null) {
//      //tmp.iterator.foreach(r //=> {
//      //  r._2.clear()
//      //})
//      tmp.clean
//      tmp = null
//    }
  }

//  private[spark] def update(value: (Short, Int)) = (hadValue: Boolean, oldValue: Set[(Short, Int)]) => {
//    if (hadValue) oldValue + value else Set(value)
//  }

//  private[spark] def update(value: Int) = (hadValue: Boolean, oldValue: EWAHCompressedBitmap) => {
//    if (hadValue) {oldValue.addWord(value);oldValue} else EWAHCompressedBitmap.bitmapOf(value)
//  }

    private[spark] def update(value: Int) = (hadValue: Boolean, oldValue: RoaringBitmap) => {
      if (hadValue) {oldValue.add(value);oldValue} else RoaringBitmap.bitmapOf(value)
    }

  override def tap(record: T) = {
    //tmp.changeValue(record._1.hashCode(), update(tContext.currentRecordInfo._2))
    //tmp.changeValue(record._1, update(tContext.currentRecordInfo))
    //addRecordInfo((splitId, newRecordId), tContext.currentRecordInfo)
//    tmp.insert(record._1.hashCode(), tContext.currentRecordInfo)
    //tmp.changeValue(record._1.hashCode(), update(tContext.currentRecordInfo))
    tmp.changeValue(Hashing.murmur3_32().hashInt(record._1.hashCode()).asInt(), update(tContext.currentRecordInfo)) //null, (c: RoaringBitmap) => c)//{c.add(tContext.currentRecordInfo); c})
    //tmp.put(Hashing.murmur3_32().hashInt(record._1.hashCode()).asInt(), tContext.currentRecordInfo)
    record
  }
}
