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
import org.apache.spark.lineage.perfdebug.perftrace.AggregateLatencyStats
import org.apache.spark.lineage.util.LongLongByteBuffer
import org.apache.spark.util.PackIntIntoLong
import org.apache.spark.util.collection.{CompactBuffer, OpenHashSet, PrimitiveKeyOpenHashMap}

import scala.reflect.ClassTag

private[spark]
class TapPostCoGroupLRDD[T: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapPostShuffleLRDD[T](lc, deps) with PostShuffleLatencyStatsTap[T]
{
  @transient private var buffer: LongLongByteBuffer = null

  override def getCachedData: Lineage[T] =
    shuffledData.setIsPostShuffleCache().asInstanceOf[Lineage[T]]
  
  override def getLatencyStats: AggregateLatencyStats =
    firstParent.asInstanceOf[CoGroupedLRDD[_]].partitionLatencyStats
  
  override def materializeBuffer: Array[Any] = {
    // TODO jteoh: I've stopped supporting cogroup for the time being (3/25/19)
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
          tmp += next._2 // unknown input of some sort of Long, perhaps input splits?
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
      // recId is per distinct key
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
        // 08/07/2018 -r._2 is the only thing changing here - each value has some sort of Long
        // associated with it. I would expect this to be some sort of split ID...?
        
        // 08/17/2018 jteoh: Here's my current understanding of what's happening here:
        // 1. each cogroup `record` consists of the key (T) and a 2D collection: dependency +
        // corresponding values from the dependency.
        // 2. We loop through the 2d collection (ie each dependency) and iterate through the
        // values, which themselves consist of actual value (from the dependency, for the given
        // key) and a Long that represents lineage information. Right now it's suspected that
        // this long is just partition ID - all further discussion will refer to these as some
        // form of input split.
        // 3. Before iterating: we generate a unique lineage ID (murmurhash) for the given key.
        // We also generate a new lineage ID for the output record from the postshuffle phase (ie
        // the [K, Array[Iter[V]]]. For a given value (from a dependency), both of these are stored
        // in the buffer along with the input partition that the value comes from.
        // 4. Later in materializeBuffer, we gather all input splits by the unique key. We then
        // output (Output id, unique key, inputSplits*). Note that these inputSplits are a union
        // of all input partitions across every dependency. Eg if dependency 1 only has records
        // from partition 0 (ie 1-0) but dependency 2 only has records from partition 2 (2-2),
        // this union would contain both (0, 2). The resulting join is slightly suboptimal
        // (trying to trace either dependency will result in trying to join/lookup records that
        // don't exist, eg partition 2 in dependency 1 (1-2) or partition 0 in dependency 2 (2-0)
        // Final note: I'm not sure why the materializeBuffer method is so complicated.
        // Based on the above understanding, it would suffice to gather all input splits across
        // the entire Array of Iterables and write that out to the buffer - effectively avoiding
        // the map generation above. It might have additional overheads though (not evaluated yet).
        r._1
      })
    }

    (key, iters.reverse).asInstanceOf[T] // jteoh: why reverse??
  }
}
