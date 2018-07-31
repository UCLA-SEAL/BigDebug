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

import org.apache.spark._
import org.apache.spark.lineage.{LAggregator, LineageContext}
import org.apache.spark.rdd.ShuffledRDD

import scala.reflect._

/**
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param previous the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 * @tparam C the combiner class.
 */

class ShuffledLRDD[K: ClassTag, V: ClassTag, C: ClassTag](
    @transient var previous: Lineage[_ <: Product2[K, V]],
    part: Partitioner
  ) extends ShuffledRDD[K, V, C](previous, part) with Lineage[(K, C)] {
  self =>

  override def ttag = classTag[(K, C)]

  override def lineageContext: LineageContext = previous.lineageContext

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    // notable differences:
    // using Some(isLineageActive) to getReader
    // passing in (isPreShuffleCache, id) as arguments to read() call.
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager
    .getReader(dep.shuffleHandle, split.index, split.index + 1, context, Some(isLineageActive))
      .read(isPreShuffleCache, id)
      .asInstanceOf[Iterator[(K, C)]]
  }

  /** Set mapSideCombine flag for RDD's shuffle. */
  override def setMapSideCombine(mapSideCombine: Boolean): ShuffledLRDD[K, V, C] = {
    this.mapSideCombine = mapSideCombine
    this
  }

  /** Set key ordering for RDD's shuffle. */
  override def setKeyOrdering(keyOrdering: Ordering[K]): ShuffledLRDD[K, V, C] = {
    this.keyOrdering = Option(keyOrdering)
    this
  }

  override def tapRight(): TapLRDD[(K, C)] = previous.withScope{
    val tap = new TapPostShuffleLRDD[(K, C)](
      lineageContext, Seq(new OneToOneDependency[(K, C)](this))
    )
    setTap(tap)
    setCaptureLineage(true)
    tap.setCached(this)
  }

  override def tapLeft(): TapLRDD[(K, C)] = previous.withScope{
    var newDeps = Seq.empty[Dependency[_]]
    for(dep <- dependencies) {
      newDeps = newDeps :+ new OneToOneDependency(dep.rdd)
    }
    new TapPreShuffleLRDD[(K, C)](lineageContext, newDeps).setCached(this).combinerEnabled(mapSideCombine)
  }

  // jteoh: this appears to not be used anywhere.
  override def getAggregate(tappedIter: Iterator[Nothing], context: TaskContext): Iterator[Product2[_, _]] = {
    if (tappedIter.isEmpty) {
      return tappedIter
    } else {
      val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
      val result: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
            dep.aggregator.get.combineCombinersByKey(tappedIter, context, true)
        } else {
            dep.aggregator.get.combineValuesByKey(tappedIter, context)
        }
      } else if (dep.aggregator.isEmpty && dep.mapSideCombine) {
        throw new IllegalStateException("Aggregator is empty for map-side combine")
      } else {
        // Convert the Product2s to pairs since this is what downstream RDDs currently expect
        tappedIter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
      }
      result
    }
  }

  override def replay(rdd: Lineage[_]) = new ShuffledLRDD[K, V, C](rdd.asInstanceOf[Lineage[(K, V)]], part)
    .setSerializer(userSpecifiedSerializer.getOrElse(null))
    .setAggregator(aggregator.get.asInstanceOf[LAggregator[K, V, C]].setLineage(false))
    .asInstanceOf[ShuffledLRDD[K, V, C]]
    .setMapSideCombine(mapSideCombine)
}
