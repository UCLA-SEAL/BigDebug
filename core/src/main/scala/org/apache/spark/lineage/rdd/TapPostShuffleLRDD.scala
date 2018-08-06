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
import org.apache.spark.lineage.util.{IntIntLongLongByteBuffer, IntKeyAppendOnlyMap}
import org.apache.spark.util.PackIntIntoLong
import org.apache.spark.util.collection.CompactBuffer

import scala.language.implicitConversions
import scala.reflect.ClassTag

private[spark]
class TapPostShuffleLRDD[T: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  @transient private var buffer: IntIntLongLongByteBuffer = _

  override def getCachedData: Lineage[T] =
    shuffledData.setIsPostShuffleCache().asInstanceOf[Lineage[T]]

  override def materializeBuffer: Array[Any] = {
    tContext synchronized {
      val result: Array[Any] = if (tContext.currentBuffer != null) {
        val map: IntKeyAppendOnlyMap[CompactBuffer[Long]] = new IntKeyAppendOnlyMap()
        val iterator = tContext.currentBuffer.iterator

        def mergeBuffer(old: CompactBuffer[Long], next: Long): CompactBuffer[Long] = {
          old += next
          old
        }

        def createBuffer(value: Long): CompactBuffer[Long] = {
          val tmp = new CompactBuffer[Long]()
          tmp += value
          tmp
        }

        while (iterator.hasNext) {
          val next = iterator.next()

          def update: (Boolean, CompactBuffer[Long]) => CompactBuffer[Long] = (hadVal, oldVal) => {
            if (hadVal) mergeBuffer(oldVal, next._1) else createBuffer(next._1)
          }

          map.changeValue(next._2, update)
        }
  
        // jteoh: refactoring ifLast check to make its usage clearer
        // jteoh: 8/6/18 - external lineage does not make an assumption of shared partitions by
        // default, so always include the split id in the output.
        // Unconfirmed: I'm not actually sure why the splitId is required in Titian if the RDD is
        // the last one in the execution graph...
        val outputIdFn: Int => Long = //if(isLast) {
          PackIntIntoLong(splitId, _)
        //} else {
        //  Int.int2long
        //}
        
        // jteoh: added timestamp and latency
        buffer.iterator.map(r => (outputIdFn(r._1), (map(r._2), r._2), r._3, r._4)).toArray
      } else {
        Array()
      }
      // We release the buffer here because not needed anymore
      releaseBuffer()

      result
    }
  }

  override def initializeBuffer() = buffer = new IntIntLongLongByteBuffer(tContext.getFromBufferPool())

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
    val timeTaken = 0L // TODO - anything useful to measure here? using the extra value bc of
    // buffer implementations (no IntIntLong) and possibility of extension.
    val postShuffleTime = System.currentTimeMillis()
    
    tContext.currentInputId = record.asInstanceOf[(_, _)]._1.hashCode()
    buffer.put(tContext.currentInputId,
      Hashing.murmur3_32().hashString(record.asInstanceOf[(_, _)]._1.toString).asInt(),
      postShuffleTime,
      timeTaken
    )
    if(isLast) {
      (record, PackIntIntoLong(splitId, tContext.currentInputId)).asInstanceOf[T]
    } else {
      record
    }
  }
}