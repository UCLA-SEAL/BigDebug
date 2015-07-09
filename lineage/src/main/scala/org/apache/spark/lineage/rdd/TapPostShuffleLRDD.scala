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

import java.util

import org.apache.spark._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.util.PackIntIntoLong

import scala.language.implicitConversions
import scala.reflect.ClassTag

private[spark]
class TapPostShuffleLRDD[T <: Product2[_, _]: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  @transient private var buffer: util.ArrayDeque[Any] = null

  override def getCachedData = shuffledData.setIsPostShuffleCache()

//  override def materializeBuffer: Array[Any] = {
//    tContext synchronized {
//      if (tContext.currentBuffer != null) {
////        val map: PrimitiveKeyOpenHashMap[Int, CompactBuffer[Long]] = new PrimitiveKeyOpenHashMap()
////        val iterator = tContext.currentBuffer.iterator
////
////        while (iterator.hasNext) {
////          val next = iterator.next()
////          map.changeValue(
////          next._2, {
////            val tmp = new CompactBuffer[Long]()
////            tmp += (next._1)
////            tmp
////          },
////          (old: CompactBuffer[Long]) => {
////            old += (next._1)
////            old
////          })
////        }
////
////        // We release the buffer here because not needed anymore
////        releaseBuffer()
//
//        tContext.currentBuffer.toArray.map(r => (r._2))
//      } else {
//        Array()
//      }
//    }
//  }

  override def materializeBuffer: Array[Any] = buffer.toArray.asInstanceOf[Array[Any]]

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

  override def releaseBuffer = {
    if(tContext.currentBuffer != null) {
//      tContext.currentBuffer.clear()
//      tContext.addToBufferPool(tContext.currentBuffer.getData)
      tContext.currentBuffer = null
    }
  }

  override def initializeBuffer() = buffer = new util.ArrayDeque[Any]()

  override def tap(record: T) = {
    val provenace = tContext.currentBuffer.apply(record._1.hashCode())
    tContext.currentInputId = PackIntIntoLong(newRecordId(), splitId) :: List(provenace)
    buffer.add(tContext.currentInputId)
    record
  }
}