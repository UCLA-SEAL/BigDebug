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

package org.apache.spark

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.collection.{CompactBuffer, AppendOnlyMap, ExternalAppendOnlyMap}

import scala.collection.mutable.ListBuffer

/**
 * :: DeveloperApi ::
 * A set of functions used to aggregate data.
 *
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */
@DeveloperApi
case class Aggregator[K, V, C] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C) {

  private val externalSorting = SparkEnv.get.conf.getBoolean("spark.shuffle.spill", true)

  @deprecated("use combineValuesByKey with TaskContext argument", "0.9.0")
  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]]): Iterator[(K, C)] =
    combineValuesByKey(iter, null)

  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]],
      context: TaskContext): Iterator[(K, C)] = {
    if (!externalSorting) {
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
      //if(context.currentRecordInfo._2.equals(0)) {
        combiners.insertAll(iter)
//      } else {
//        var kv: Product2[K, V] = null
//        while (iter.hasNext) {
//          kv = iter.next()
//          combiners.insert(kv._1, kv._2)
//          //context.currentRecordInfos.asInstanceOf[ExternalAppendOnlyMap[Int, Int, _]].insert(kv._1.hashCode(), context.currentRecordInfo._2) // Matteo
//        }
//      }
      // Update task metrics if context is not null
      // TODO: Make context non optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.memoryBytesSpilled += combiners.memoryBytesSpilled
        c.taskMetrics.diskBytesSpilled += combiners.diskBytesSpilled
      }

      // If currentRecordInfo is zero then no lineage is active. Added by Matteo
      if(context.currentRecordInfo.equals(0)) {
        combiners.iterator
      } else {
        combiners.iterator.zipWithIndex.map(r => {
          (r._1._1, (r._1._2, (context.stageId.toShort, context.partitionId.toShort, r._2))).asInstanceOf[(K, C)]
        })
      }
    }
  }

  @deprecated("use combineCombinersByKey with TaskContext argument", "0.9.0")
  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]]) : Iterator[(K, C)] =
    combineCombinersByKey(iter, null)

  private[spark] def update(value: (Short, Short, Int)) = (hadValue: Boolean, oldValue: ListBuffer[(Short, Short, Int)]) => {
    if (hadValue) oldValue += value else ListBuffer(value)
  }

      def mergeCombiners(c1: ListBuffer[(Short, Short, Int)], c2: ListBuffer[(Short, Short, Int)]):  ListBuffer[(Short, Short, Int)] = c1 ++= c2

      def mergeValues(c: ListBuffer[(Short, Short, Int)], e: (Short, Short, Int)):  ListBuffer[(Short, Short, Int)] = {
        c += (e)
      }

  def combineCombinersByKey(iter: Iterator[_ <: Product2[K, C]], context: TaskContext)
      : Iterator[(K, C)] =
  {
    if (!externalSorting) {
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

      if(context.currentRecordInfo.equals(0)) { // Matteo
        while (iter.hasNext) {
          val pair = iter.next()
          combiners.insert(pair._1, pair._2)
        }
      } else {
        System.gc()
        println(Runtime.getRuntime.freeMemory())
        var pair: Product2[K, Product2[C, (Short, Short, Int)]] = null
        while (iter.hasNext) {
          pair = iter.next().asInstanceOf[Product2[K, Product2[C, (Short, Short, Int)]]]
          combiners.insert(pair._1, pair._2._1)
          context.currentRecordInfos.changeValue(pair._1.hashCode(), CompactBuffer(pair._2._2), (old: CompactBuffer[(Short, Short, Int)]) => old += pair._2._2)
          //context.tmp += ((pair._2._2, pair._1.hashCode()))
        }
      }
      // Update task metrics if context is not null
      // TODO: Make context non-optional in a future release
      Option(context).foreach { c =>
        c.taskMetrics.memoryBytesSpilled += combiners.memoryBytesSpilled
        c.taskMetrics.diskBytesSpilled += combiners.diskBytesSpilled
      }
      combiners.iterator.zipWithIndex.asInstanceOf[Iterator[(K, C)]]
    }
  }
}
