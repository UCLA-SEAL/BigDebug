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
import org.apache.spark.lineage.perfdebug.perftrace.AggregateLatencyStats
import org.apache.spark.lineage.util.{IntIntIntByteBuffer, IntIntLongByteBuffer}
import org.apache.spark.lineage.{Int2RoaringBitMapOpenHashMap, LineageContext}
import org.apache.spark.util.PackIntIntoLong
import org.roaringbitmap.longlong.Roaring64NavigableMap

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.reflect.ClassTag
import collection.JavaConverters._

private[spark]
class TapPreShuffleLRDD[T <: Product2[_, _]: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) with PreShuffleLatencyStatsTap[T] {

  @transient private var buffer: IntIntIntByteBuffer = _
  @transient private var latencyStats: AggregateLatencyStats = _
  override def getLatencyStats: AggregateLatencyStats = latencyStats
  override def setLatencyStats(stats: AggregateLatencyStats): Unit = latencyStats = stats
  
  override def getCachedData: Lineage[T] =
    shuffledData.setIsPreShuffleCache().asInstanceOf[Lineage[T]]

  override def materializeBuffer: Array[Any] = {
    //materializeBufferWithListBuffers
    materializeBufferLongBitmap
  }
  
  def materializeBufferWithListBuffers: Array[Any] = {
    // Titian previously used an Int2RoaringBitMapOpenHashMap to store each outputID (key) and
    // the (unordered) set of inputIDs (value = integer set). With the addition of latency, the
    // value must now change to a set of (integer, long) which is not supported by the existing
    // data structure. Instead, we opt for Scala's OpenHashMap, which allows for size
    // specification (to avoid resizing).
    //
    // side note: if latency is reduced to ints, we could also combine inputID+latency into a
    // single long. To my knowledge, there is still no hyperoptimized roaringbitmap equivalent
    // for longs.
    val mapCapacity = 16384 // taken from original Int2RoaringBitMapOpenHashMap
    val outputToInputAndLatency= new mutable.OpenHashMap[Int, ListBuffer[(Int, Long)]](mapCapacity)

    val iterator = buffer.iterator
    while(iterator.hasNext) {
      val next = iterator.next()
      val key = next._1
      val inputIdsWithLatencies = outputToInputAndLatency.getOrElseUpdate(key,
                                                           new ListBuffer[(Int, Long)]())
      inputIdsWithLatencies += Tuple2(next._2, next._3)
    }

    // We release the buffer here because not needed anymore
    releaseBuffer()

    outputToInputAndLatency.map({case (outputId, inputsWithLats) =>
      Tuple2(Tuple2(splitId.toInt, outputId), inputsWithLats)
    }).toArray
  }
  
  def materializeBufferLongBitmap: Array[Any] = {
    // Titian previously used an Int2RoaringBitMapOpenHashMap to store each outputID (key) and
    // the (unordered) set of inputIDs (value = integer set). With the addition of latency, the
    // value must now change to a set of (integer, long). We use Roaring64NavigableMap, as
    // described at https://github.com/RoaringBitmap/RoaringBitmap#64-bit-integers-long. To do
    // so, we also cast the Long latency to a single int and combine latency and ID into a single
    // Long.
    val mapCapacity = 16384 // taken from original Int2RoaringBitMapOpenHashMap
    // Consider initializing with values too:
    // org.roaringbitmap.longlong.Roaring64NavigableMap.bitmapOf()
    val outputToInputAndLatency= new mutable.OpenHashMap[Int, Roaring64NavigableMap](mapCapacity)
    
    val iterator = buffer.iterator
    while(iterator.hasNext) {
      val next = iterator.next()
      val key = next._1
      val inputID = next._2
      val latency = next._3
      val record = PackIntIntoLong.apply(inputID, latency.toInt)
      
      val inputIDsWithLatencies = outputToInputAndLatency.getOrElseUpdate(key,
                                                                          new Roaring64NavigableMap())
      inputIDsWithLatencies.addLong(record)
    }
    
    // We release the buffer here because not needed anymore
    releaseBuffer()
    
    outputToInputAndLatency.map({case (outputId, inputsWithLats) =>
      Tuple2(Tuple2(splitId.toInt, outputId), inputsWithLats.iterator().asScala.toSeq)
    }).toArray
  }

  override def initializeBuffer() = buffer = new IntIntIntByteBuffer(tContext.getFromBufferPoolLarge())

  override def releaseBuffer() = {
    if(buffer != null) {
      buffer.clear()
      tContext.addToBufferPoolLarge(buffer.getData)
      buffer = null
    }
  }
  //println("WARNING: TAPPRESHUFFLELRDD TAP IS DISABLED")
  override def tap(record: T) = {
    val timeTaken = tContext.getSummedRddRecordTime()
    buffer.put( Hashing.murmur3_32().hashString(record._1.toString).asInt(),
                tContext.currentInputId,
                timeTaken // jteoh
    )
    record
  }
  @deprecated
  private def materializeBufferOld: Array[Any] = {
    // edit 3/1/19: This is incorrect but retained for easier comparison with the original Titian
    // code (+ some modifications I made before realizing a crucial error).
    
    // jteoh: Added two map to separately track record tap time (pre-shuffle) and record time up
    // to the preshuffle. (edit 3/1/19: This is outdated, there's only really one latency)
    // values for the three maps are: currentInputId (often incremental), current system time, or
    // individual record latency.
    // latter two maps may be suboptimal - will need to analyze and see if there's anything that can
    // be done to improve. One option may be reducing precision to milliseconds or even seconds.
    // Another may be casting into the int range and then using the bitmap approach again.
  
    
    val map = new Int2RoaringBitMapOpenHashMap(16384)
    val recordTimeMap = new mutable.HashMap[Int, ArrayBuffer[Long]]
    val iterator = buffer.iterator
  
    while(iterator.hasNext) {
      val next = iterator.next()
      val key = next._1
      map.put(key, next._2)
      recordTimeMap.getOrElseUpdate(key, new ArrayBuffer[Long]()) += next._3
    }
  
    // We release the buffer here because not needed anymore
    releaseBuffer()
  
    map.keySet().toIntArray.map(k =>
                                  // jteoh: converted from tuple2 to tuple3 which includes array of latencies
                                  //new Tuple2(new Tuple2(splitId.toInt, k), map.get(k).toArray)).toArray
                                  Tuple3(Tuple2(splitId.toInt, k),
                                         map.get(k).toArray, // all input partitions, with potential duplicates depending on
                                         // the original dependency.
                                         recordTimeMap(k).toArray)).toArray
    // final output = list of:
    /*(split, murmurhash of record key(?)),
    titian-labeled inputs for that particular key (from tap method/task context)
    timestamps for each appearance of that key in the tap method
    record latencies for each appearance of that key in the tap method */
  
  }
}
