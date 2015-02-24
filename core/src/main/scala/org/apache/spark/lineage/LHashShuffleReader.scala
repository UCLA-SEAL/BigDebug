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

import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.shuffle.hash.{BlockStoreShuffleFetcher, HashShuffleReader}
import org.apache.spark.util.collection.{CompactBuffer, PrimitiveKeyOpenHashMap, ExternalSorter}
import org.apache.spark.{TaskContextImpl, InterruptibleIterator, TaskContext}

import scala.collection.mutable.ListBuffer

private[spark] class LHashShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    var lineage: Boolean = false
  ) extends HashShuffleReader[K, C](handle, startPartition, endPartition, context)
{
  require(endPartition == startPartition + 1,
    "Hash shuffle currently only supports fetching one partition")

  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task */
  override def read(isCache: Option[Boolean] = None, shuffId: Int = 0): Iterator[Product2[K, C]] = {
    val ser = Serializer.getSerializer(dep.serializer)
    val tappedIter = BlockStoreShuffleFetcher.fetch(handle.shuffleId, startPartition, context, ser)

    if(isCache.isDefined) {
      if(isCache.get) {
        return tappedIter
      } else {
        lineage = true
      }
    }

    context.asInstanceOf[TaskContextImpl].currentRecordInfos = new PrimitiveKeyOpenHashMap[Int, CompactBuffer[(Short, Short, Int)]]

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if(lineage) {
        context.asInstanceOf[TaskContextImpl].currentRecordInfo = 1 // TODO
      }
      if (dep.mapSideCombine) {
        new InterruptibleIterator(context, dep.aggregator.get.combineCombinersByKey(tappedIter, context))
      } else {
        new InterruptibleIterator(context,
          dep.aggregator.get.combineValuesByKey(tappedIter, context))
      }
    } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
      throw new IllegalStateException("Aggregator is empty for map-side combine")
    } else {
      // Convert the Product2s to pairs since this is what downstream RDDs currently expect
      tappedIter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        val sorter = new ExternalSorter[K, C, C](ordering = Some(keyOrd), serializer = Some(ser))
        sorter.insertAll(aggregatedIter)
        context.taskMetrics.memoryBytesSpilled += sorter.memoryBytesSpilled
        context.taskMetrics.diskBytesSpilled += sorter.diskBytesSpilled
        sorter.iterator
      case None =>
        aggregatedIter
    }
  }

  private[spark] def update(value: (Short, Short, Int)) = (hadValue: Boolean, oldValue: ListBuffer[(Short, Short, Int)]) => {
    if (hadValue) oldValue += value else new ListBuffer += value
  }

//  def untap[T](iter : Iterator[_ <: Product2[K, Product2[_, (Short, Short, Int)]]]) = {
//    if(lineage) {
//      iter.map(r => {
//        context.currentRecordInfos.changeValue(r._1.hashCode(),update(r._2._2))
//        (r._1, r._2._1).asInstanceOf[T]
//      })
//    } else {
//      iter.asInstanceOf[Iterator[T]]
//    }
//  }
//
//  def tap(iter: Iterator[Product2[K, C]]): Iterator[Product2[K, C]] = {
//    if(lineage) {
//      iter.zipWithIndex.asInstanceOf[Iterator[Product2[K, C]]]
//    } else {
//      iter
//    }
//  }
}
