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
import org.apache.spark.lineage.LineageContext
import org.apache.spark.util.collection.{CompactBuffer, PrimitiveKeyOpenHashMap}

import scala.reflect.ClassTag

private[spark]
class TapPostCoGroupLRDD[T <: Product2[_, _]: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapPostShuffleLRDD[T](lc, deps)
{
  @transient private var buffer: PrimitiveKeyOpenHashMap[Int, CompactBuffer[List[_]]] = null

  override def getCachedData = shuffledData.setIsPostShuffleCache()

//  override def materializeBuffer: Array[Any] = {
//    if(buffer != null) {
//      val map: PrimitiveKeyOpenHashMap[Int, CompactBuffer[Long]] = new PrimitiveKeyOpenHashMap()
//      val iterator = buffer.iterator
//
//      while (iterator.hasNext) {
//        val next = iterator.next()
//        map.changeValue(
//        next._2, {
//          val tmp = new CompactBuffer[Long]()
//          tmp += next._1
//          tmp
//        },
//        (old: CompactBuffer[Long]) => {
//          old += next._1
//          old
//        })
//      }
//
//      // We release the buffer here because not needed anymore
//      releaseBuffer()
//
//      map.toArray.zipWithIndex.map(r => (r._2, (r._1._2, r._1._1)))
//    } else {
//      Array()
//    }
//  }

  override def initializeBuffer() = buffer = new PrimitiveKeyOpenHashMap[Int, CompactBuffer[List[_]]]

  override def releaseBuffer() = {
    if(buffer != null) {
      //buffer.clear()
      //tContext.addToBufferPool(buffer.getData)
      buffer = null
    }
  }

  override def compute(split: Partition, context: TaskContext) = {
    if(tContext == null) {
      tContext = context.asInstanceOf[TaskContextImpl]
    }
    splitId = split.index.toShort
    nextRecord = -1

    initializeBuffer()

    //    SparkEnv.get.cacheManager.asInstanceOf[LCacheManager].initMaterialization(this, split, context)

    firstParent[T].iterator(split, context).map(tap)
  }

  override def tap(record: T) = {
    val (key, values) = record.asInstanceOf[(Any, Array[Iterable[(_, List[_])]])]
    val hash = key.hashCode
    val iters = for(iter <- values) yield {
      iter.map(r => {
        buffer.changeValue(hash, CompactBuffer(r._2), (old: CompactBuffer[List[_]]) => old += r._2)
        r._1
      })
    }
    val provenace = buffer.apply(hash)
    tContext.currentInputId = newRecordId() :: List(provenace)

    (key, iters.reverse).asInstanceOf[T]
  }
}
