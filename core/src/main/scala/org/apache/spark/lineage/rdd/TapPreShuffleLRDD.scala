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
import org.apache.spark.lineage.{Int2RoaringBitMapOpenHashMap, LineageContext}
import org.apache.spark.lineage.util.{IntIntByteBuffer, IntIntLongByteBuffer}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

private[spark]
class TapPreShuffleLRDD[T <: Product2[_, _]: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  @transient private var buffer: IntIntLongByteBuffer = _

  override def getCachedData: Lineage[T] =
    shuffledData.setIsPreShuffleCache().asInstanceOf[Lineage[T]]

  override def materializeBuffer: Array[Any] = {
    // jteoh: Added second map to separately track record tap time (pre-shuffle).
    // for both: value is either currentInputId (often incremental) or current system time
    // second map may be suboptimal - will need to analyze and see if there's anything that can
    // be done to improve. One option may be reducing precision to milliseconds or even seconds.
    // Another may be casting into the int range and then using the bitmap approach again.
    val map = new Int2RoaringBitMapOpenHashMap(16384)
    val startTimeMap = new mutable.HashMap[Int, ListBuffer[Long]]
    val iterator = buffer.iterator

    while(iterator.hasNext) {
      val next = iterator.next()
      val key = next._1
      map.put(key, next._2)
      startTimeMap.getOrElseUpdate(key, new ListBuffer[Long]()) += next._3
    }

    // We release the buffer here because not needed anymore
    releaseBuffer()

    map.keySet().toIntArray.map(k =>
      // jteoh: converted from tuple2 to tuple3 which includes array of times.
      //new Tuple2(new Tuple2(splitId.toInt, k), map.get(k).toArray)).toArray
      new Tuple3(new Tuple2(splitId.toInt, k), map.get(k).toArray, startTimeMap(k).toList)).toArray
  }

  override def initializeBuffer() = buffer = new IntIntLongByteBuffer(tContext.getFromBufferPoolLarge())

  override def releaseBuffer() = {
    if(buffer != null) {
      buffer.clear()
      tContext.addToBufferPoolLarge(buffer.getData)
      buffer = null
    }
  }

  override def tap(record: T) = {
    val preShuffleTime = System.nanoTime()
    buffer.put( Hashing.murmur3_32().hashString(record._1.toString).asInt(),
                tContext.currentInputId,
                preShuffleTime)
    record
  }
}
