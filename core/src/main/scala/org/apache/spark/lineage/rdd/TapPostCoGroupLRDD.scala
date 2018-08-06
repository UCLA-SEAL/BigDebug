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
import org.apache.spark.lineage.util.LongLongByteBuffer
import org.apache.spark.util.PackIntIntoLong
import org.apache.spark.util.collection.{CompactBuffer, OpenHashSet, PrimitiveKeyOpenHashMap}

import scala.reflect.ClassTag

private[spark]
class TapPostCoGroupLRDD[T: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapPostShuffleLRDD[T](lc, deps)
{
  @transient private var buffer: LongLongByteBuffer = null

  override def getCachedData: Lineage[T] =
    shuffledData.setIsPostShuffleCache().asInstanceOf[Lineage[T]]

  override def materializeBuffer: Array[Any] = {
    if(buffer != null) {
      val map: PrimitiveKeyOpenHashMap[Int, CompactBuffer[Long]] = new PrimitiveKeyOpenHashMap()
      val iterator = buffer.iterator
      val set = new OpenHashSet[Long]()

      while (iterator.hasNext) {
        val next = iterator.next()
        set.add(next._1) // next._1 = (murmurhash, outputRecId) as Long
        map.changeValue( // map: murmurHash -> [outputLineageIds])
        PackIntIntoLong.getLeft(next._1), {
          val tmp = new CompactBuffer[Long]()
          tmp += next._2
          tmp
        },
        (old: CompactBuffer[Long]) => {
          old += next._2
          old
        })
      }

      // We release the buffer here because not needed anymore
      releaseBuffer()
  
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
      
      set.iterator
      .map(r => (PackIntIntoLong.getLeft(r), PackIntIntoLong.getRight(r))) //murmurhash, outputRecId
      .map(r => (outputIdFn(r._2), (map.getOrElse(r._1, null), r._1))
      ).toArray // ( OutputLinId(Partition,RecId), (CompactBuf[InpLinIds], murmurHash) )
    } else {
      Array()
    }
  }

  override def initializeBuffer() = buffer = new LongLongByteBuffer(tContext.getFromBufferPool())

  override def releaseBuffer() = {
    if(buffer != null) {
      buffer.clear()
      tContext.addToBufferPool(buffer.getData)
      buffer = null
    }
  }

  override def tap(record: T) = {
    val (key, values) = record.asInstanceOf[(T, Array[Iterable[(_, Long)]])]
    val hash = Hashing.murmur3_32().hashString(key.toString).asInt()
    tContext.currentInputId = newRecordId()
    val iters = for(iter <- values) yield {
      iter.map(r => {
        buffer.put(PackIntIntoLong(hash, nextRecord), r._2) // TODO jteoh: figure out where Longs
        // come from
        //(murmurHash, outputId), all Longs - I'm guessing lineage IDs but not sure how they're
        // introduced...
        r._1
      })
    }

    (key, iters.reverse).asInstanceOf[T] // jteoh: why reverse??
  }
}
