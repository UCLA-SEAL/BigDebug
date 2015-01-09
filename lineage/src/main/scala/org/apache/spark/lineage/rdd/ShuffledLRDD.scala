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
import org.apache.spark.lineage.LineageContext
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

class ShuffledLRDD[K, V, C](
    @transient var previous: Lineage[_ <: Product2[K, V]],
    part: Partitioner
  ) extends ShuffledRDD[K, V, C](previous, part) with Lineage[(K, C)] {

  override def ttag = classTag[(K, C)]

  override def lineageContext: LineageContext = previous.lineageContext

  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K, V, C]]
    SparkEnv.get.shuffleManager
    .getReader(dep.shuffleHandle, split.index, split.index + 1, context, Some(isLineageActive))
      .read(isPreShuffleCache, id)
      .asInstanceOf[Iterator[(K, C)]]
  }

  override def tapRight(): TapLRDD[(K, C)] = {
    val tap = new TapPostShuffleLRDD[(K, C)](
      lineageContext, Seq(new OneToOneDependency[(K, C)](this))
    )
    setTap(tap)
    setCaptureLineage(true)
    tap.setCached(this)
  }

  override def tapLeft(): TapLRDD[(K, C)] = {
    var newDeps = Seq.empty[Dependency[_]]
    for(dep <- dependencies) {
      newDeps = newDeps :+ new OneToOneDependency(dep.rdd)
    }
    new TapPreShuffleLRDD[(K, C)](lineageContext, newDeps).setCached(this)
  }
}
