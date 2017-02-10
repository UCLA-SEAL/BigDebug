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

package org.apache.spark.lineage

import com.google.common.hash.Hashing
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection._
import org.apache.spark.{Aggregator, TaskContext, TaskContextImpl}

/**
 * :: DeveloperApi ::
 * A set of functions used to aggregate data.
 *
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */
@DeveloperApi
class LAggregator[K, V, C] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C,
    var isLineage: Boolean = false)
  extends Aggregator[K, V, C](createCombiner, mergeValue, mergeCombiners) {

  override def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]], context: TaskContext)
      : Iterator[(K, C)] = {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K,C]
      var kv: Product2[K, V] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeValue(oldValue, kv._2) else createCombiner(kv._2)
      }
      while (iter.hasNext) {
        kv = iter.next()
        combiners.changeValue(kv._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, V, C](createCombiner, mergeValue, mergeCombiners)

      if(!isLineage) {
        combiners.insertAll(iter)
      } else {
        var pair: Product2[K, Product2[V, Long]] = null
        val buffer = new LongIntByteBuffer(context.asInstanceOf[TaskContextImpl].getFromBufferPool())

        val update: (Boolean, C) => C = (hadVal, oldVal) => {
          if (hadVal) mergeValue(oldVal, pair._2._1) else createCombiner(pair._2._1)
        }

        while (iter.hasNext) {
          pair = iter.next().asInstanceOf[Product2[K, Product2[V, Long]]]
          combiners.insert(pair._1, update)
          buffer.put(pair._2._2, Hashing.murmur3_32().hashString(pair._1.toString).asInt())
        }
        context.asInstanceOf[TaskContextImpl].currentBuffer = buffer
      }

      // Update task metrics if context is not null
      // TODO: Make context non optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.memoryBytesSpilled += combiners.memoryBytesSpilled
        c.taskMetrics.diskBytesSpilled += combiners.diskBytesSpilled
      }
      combiners.iterator

    }
  }

  override def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]], context: TaskContext, isCache: Boolean = false)
    : Iterator[(K, C)] = {
    if (!isSpillEnabled) {
      val combiners = new AppendOnlyMap[K,C]
      var kc: Product2[K, C] = null
      val update = (hadValue: Boolean, oldValue: C) => {
        if (hadValue) mergeCombiners(oldValue, kc._2) else kc._2
      }
      while (iter.hasNext) {
        kc = iter.next()
        combiners.changeValue(kc._1, update)
      }
      combiners.iterator
    } else {
      val combiners = new ExternalAppendOnlyMap[K, C, C](identity, mergeCombiners, mergeCombiners)
      if(!isLineage) {
        while (iter.hasNext) {
          val pair = iter.next()
          combiners.insert(pair._1, pair._2)
        }
      } else {
        var pair: Product2[K, Product2[C, Long]] = null
        val tappedIter = iter.asInstanceOf[Iterator[_ <: Product2[K, Product2[C, Long]]]]
        if(isCache) {
          while (iter.hasNext) {
            pair = tappedIter.next()
            combiners.insert(pair._1, pair._2._1)
          }
        } else {
          val buffer = new LongIntByteBuffer(context.asInstanceOf[TaskContextImpl].getFromBufferPool())

         // val tmp = new OpenHashMap[Int, String]()
          while (iter.hasNext) {
            pair = tappedIter.next()
            combiners.insert(pair._1, pair._2._1)
            buffer.put(pair._2._2, Hashing.murmur3_32().hashString(pair._1.toString).asInt())
          }
          context.asInstanceOf[TaskContextImpl].currentBuffer = buffer
        }
      }

      // Update task metrics if context is not null
      // TODO: Make context non-optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.memoryBytesSpilled += combiners.memoryBytesSpilled
        c.taskMetrics.diskBytesSpilled += combiners.diskBytesSpilled
      }
      combiners.iterator
    }
  }

  def setLineage(lineage: Boolean) = {
    isLineage = lineage
    this.asInstanceOf[Aggregator[K, V, C]]
  }
}
