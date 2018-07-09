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
import org.apache.spark.lineage.util.{IntIntByteBuffer, IntIntLongLongByteBuffer}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

private[spark]
class TapPreShuffleLRDD[T <: Product2[_, _]: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  @transient private var buffer: IntIntLongLongByteBuffer = _

  override def getCachedData: Lineage[T] =
    shuffledData.setIsPreShuffleCache().asInstanceOf[Lineage[T]]

  override def materializeBuffer: Array[Any] = {
    // jteoh: Added two map to separately track record tap time (pre-shuffle) and record time up
    // to the preshuffle.
    // values for the three maps are: currentInputId (often incremental), current system time, or
    // individual record latency.
    // latter two maps may be suboptimal - will need to analyze and see if there's anything that can
    // be done to improve. One option may be reducing precision to milliseconds or even seconds.
    // Another may be casting into the int range and then using the bitmap approach again.
    val map = new Int2RoaringBitMapOpenHashMap(16384)
    val startTimeMap = new mutable.HashMap[Int, ListBuffer[Long]]
    val recordTimeMap = new mutable.HashMap[Int, ListBuffer[Long]]
    val iterator = buffer.iterator

    while(iterator.hasNext) {
      val next = iterator.next()
      val key = next._1
      map.put(key, next._2)
      startTimeMap.getOrElseUpdate(key, new ListBuffer[Long]()) += next._3
      recordTimeMap.getOrElseUpdate(key, new ListBuffer[Long]()) += next._4
    }

    // We release the buffer here because not needed anymore
    releaseBuffer()

    map.keySet().toIntArray.map(k =>
      // jteoh: converted from tuple2 to tuple3 which includes array of times.
      //new Tuple2(new Tuple2(splitId.toInt, k), map.get(k).toArray)).toArray
      Tuple4(Tuple2(splitId.toInt, k),
             map.get(k).toArray,
             startTimeMap(k).toList,
             recordTimeMap(k).toList)).toArray
    // final output = list of:
    /*(split, murmurhash of record key(?)),
    titian-labeled inputs for that particular key (from tap method/task context)
    timestamps for each appearance of that key in the tap method
    record latencies for each appearance of that key in the tap method */
    
  }

  override def initializeBuffer() = buffer = new IntIntLongLongByteBuffer(tContext.getFromBufferPoolLarge())

  override def releaseBuffer() = {
    if(buffer != null) {
      buffer.clear()
      tContext.addToBufferPoolLarge(buffer.getData)
      buffer = null
    }
  }

  override def tap(record: T) = {
    val preShuffleTime = System.nanoTime()
    val timeTaken = tContext.getSummedRddRecordTime()
    buffer.put( Hashing.murmur3_32().hashString(record._1.toString).asInt(),
                tContext.currentInputId,
                preShuffleTime, // jteoh
                timeTaken // jteoh
    )
    record
  }
}
