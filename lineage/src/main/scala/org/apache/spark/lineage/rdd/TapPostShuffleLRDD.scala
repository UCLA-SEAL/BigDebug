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
import org.apache.spark._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.util.IntIntByteBuffer
import org.apache.spark.util.PackIntIntoLong
import org.apache.spark.util.collection.{CompactBuffer, PrimitiveKeyOpenHashMap}

import scala.language.implicitConversions
import scala.reflect.ClassTag

private[spark]
class TapPostShuffleLRDD[T: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  @transient private var buffer: IntIntByteBuffer = _

  override def getCachedData: Lineage[T] =
    shuffledData.setIsPreShuffleCache().asInstanceOf[Lineage[T]]

  override def materializeBuffer: Array[Any] = {
    tContext synchronized {
      val result: Array[Any] = if (tContext.currentBuffer != null) {
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

        if(isLast) {
          buffer.iterator.map(r => (PackIntIntoLong(splitId, r._1), (map(r._2), r._2))).toArray
        } else {
          buffer.iterator.map(r => (r._1, (map(r._2), r._2))).toArray
        }
      } else {
        Array()
      }
      // We release the buffer here because not needed anymore
      releaseBuffer()

      result
    }
  }

  override def initializeBuffer() = buffer = new IntIntByteBuffer(tContext.getFromBufferPool())

  override def releaseBuffer = {
    if(tContext.currentBuffer != null) {
      tContext.currentBuffer.clear()
      tContext.addToBufferPool(tContext.currentBuffer.getData)
      tContext.currentBuffer = null
    }
    if(buffer != null) {
      buffer.clear()
      tContext.addToBufferPool(buffer.getData)
      buffer = null
    }
  }

  override def tap(record: T) = {
    tContext.currentInputId = newRecordId()
    buffer.put(tContext.currentInputId, Hashing.murmur3_32().hashString(record.asInstanceOf[(_, _)]._1.toString).asInt())
    if(isLast) {
      (record, PackIntIntoLong(splitId, tContext.currentInputId)).asInstanceOf[T]
    } else {
      record
    }
  }
}