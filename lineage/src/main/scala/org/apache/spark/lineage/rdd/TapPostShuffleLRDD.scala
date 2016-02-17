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

import scala.language.implicitConversions
import scala.reflect.ClassTag

private[spark]
class TapPostShuffleLRDD[T: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  override def getCachedData: Lineage[T] =
    shuffledData.setIsPreShuffleCache().asInstanceOf[Lineage[T]]

  override def materializeBuffer: Array[Any] = {
    tContext synchronized {
      if (tContext.currentBuffer != null) {
        val map: PrimitiveKeyOpenHashMap[Int, CompactBuffer[Int]] = new PrimitiveKeyOpenHashMap()
        val iterator = tContext.currentBuffer.iterator

        while (iterator.hasNext) {
          val next = iterator.next()
          map.changeValue(
          next._2, {
            val tmp = new CompactBuffer[Int]()
            tmp += (next._1)
            tmp
          },
          (old: CompactBuffer[Int]) => {
            old += (next._1)
            old
          })
        }

        // We release the buffer here because not needed anymore
        releaseBuffer()

        map.toArray.zipWithIndex.map(r => (r._2, (r._1._2, r._1._1)))
      } else {
        Array()
      }
    }
  }

  override def releaseBuffer = {
    if(tContext.currentBuffer != null) {
      tContext.currentBuffer.clear()
      tContext.addToBufferPool(tContext.currentBuffer.getData)
      tContext.currentBuffer = null
    }
  }

  override def tap(record: T) = {
    tContext.currentInputId = newRecordId()
    if(isLast) {
      (record, tContext.currentInputId).asInstanceOf[T]
    } else {
      record
    }
  }
}