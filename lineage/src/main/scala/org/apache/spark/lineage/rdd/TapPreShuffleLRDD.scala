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
import org.apache.spark.Dependency
import org.apache.spark.lineage.util.IntIntByteBuffer
import org.apache.spark.lineage.{Int2RoaringBitMapOpenHashMap, LineageContext}

import scala.reflect.ClassTag

private[spark]
class TapPreShuffleLRDD[T <: Product2[_, _]: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  @transient private var buffer: IntIntByteBuffer = _

  override def getCachedData: Lineage[T] =
    shuffledData.setIsPreShuffleCache().asInstanceOf[Lineage[T]]

  override def materializeBuffer: Array[Any] = {
    val map = new Int2RoaringBitMapOpenHashMap(16384)
    val iterator = buffer.iterator

    while(iterator.hasNext) {
      val next = iterator.next()
      map.put(next._1, next._2)
    }

    // We release the buffer here because not needed anymore
    releaseBuffer()

    map.keySet().toIntArray.map(k =>
      new Tuple2(new Tuple2(splitId.toInt, k), map.get(k).toArray)).toArray
  }

  override def initializeBuffer() = buffer = new IntIntByteBuffer(tContext.getFromBufferPoolLarge())

  override def releaseBuffer() = {
    if(buffer != null) {
      buffer.clear()
      tContext.addToBufferPoolLarge(buffer.getData)
      buffer = null
    }
  }

  override def tap(record: T) = {
    buffer.put(Hashing.murmur3_32().hashString(record._1.toString).asInt(), tContext.currentInputId)
    record
    // TODO: if there's no shuffle after this operation, we need to map with:
    // (value, splitID + keyHash)
  }
}
