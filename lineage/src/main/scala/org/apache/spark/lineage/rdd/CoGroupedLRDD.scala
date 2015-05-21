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
import org.apache.spark.lineage.rdd.PairLRDDFunctions._
import org.apache.spark.rdd._
import org.apache.spark.util.collection.AppendOnlyMap

import scala.collection.mutable.{ArrayBuffer, Stack}
import scala.language.existentials
import scala.reflect._

/**
 * :: DeveloperApi ::
 * A RDD that cogroups its parents. For each key k in parent RDDs, the resulting RDD contains a
 * tuple with the list of values for that key.
 *
 * Note: This is an internal API. We recommend users use RDD.coGroup(...) instead of
 * instantiating this directly.

 * @param lrdds parent RDDs.
 * @param part partitioner used to partition the shuffle output
 */

class CoGroupedLRDD[K: ClassTag](var lrdds: Seq[RDD[_ <: Product2[K, _]]], part: Partitioner)
  extends CoGroupedRDD[K](lrdds, part) with Lineage[(K, Array[Iterable[_]])]
{
  override def lineageContext = lrdds.head.lineageContext

  override def ttag: ClassTag[(K, Array[Iterable[_]])] = classTag[(K, Array[Iterable[_]])]

  private[spark] var newDeps = new Stack[Dependency[_]]

  private[spark] def computeTapDependencies() =
    dependencies.foreach(dep => newDeps.push(new OneToOneDependency(dep.rdd)))

  // Copied from CoGroupRDD
  override def compute(s: Partition, context: TaskContext): Iterator[(K, Array[Iterable[_]])] = {
    val sparkConf = SparkEnv.get.conf
    val externalSorting = sparkConf.getBoolean("spark.shuffle.spill", true)
    val split = s.asInstanceOf[CoGroupPartition]
    val numRdds = split.deps.size

    // A list of (rdd iterator, dependency number) pairs
    val rddIterators = new ArrayBuffer[(Iterator[Product2[K, Any]], Int)]
    for ((dep, depNum) <- split.deps.zipWithIndex) dep match {
      case NarrowCoGroupSplitDep(rdd, _, itsSplit) =>
        // Read them from the parent
        val it = rdd.iterator(itsSplit, context).asInstanceOf[Iterator[Product2[K, Any]]]
        rddIterators += ((it, depNum))

      case ShuffleCoGroupSplitDep(handle) =>
        // Read map outputs of shuffle
        val it = SparkEnv.get.shuffleManager
          .getReader(handle, split.index, split.index + 1, context, Some(isLineageActive))
          .read(isPreShuffleCache, id)
        rddIterators += ((it, depNum))
    }

    if (!externalSorting) {
      val map = new AppendOnlyMap[K, CoGroupCombiner]
      val update: (Boolean, CoGroupCombiner) => CoGroupCombiner = (hadVal, oldVal) => {
        if (hadVal) oldVal else Array.fill(numRdds)(new CoGroup)
      }
      val getCombiner: K => CoGroupCombiner = key => {
        map.changeValue(key, update)
      }
      rddIterators.foreach { case (it, depNum) =>
        while (it.hasNext) {
          val kv = it.next()
          getCombiner(kv._1)(depNum) += kv._2
        }
      }
      new InterruptibleIterator(context,
        map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
    } else {
      val map = createExternalMap(numRdds)
      for ((it, depNum) <- rddIterators) {
        map.insertAll(it.map(pair => (pair._1, new CoGroupValue(pair._2, depNum))))
      }
      context.taskMetrics.memoryBytesSpilled += map.memoryBytesSpilled
      context.taskMetrics.diskBytesSpilled += map.diskBytesSpilled
      new InterruptibleIterator(context,
        map.iterator.asInstanceOf[Iterator[(K, Array[Iterable[_]])]])
    }
  }

  override def tapRight(): TapLRDD[(K, Array[Iterable[_]])] = {
    val tap = new TapPostCoGroupLRDD[(K, Array[Iterable[_]])](
      lineageContext, Seq(new OneToOneDependency[(K, Array[Iterable[_]])](this))
    )
    setTap(tap)
    setCaptureLineage(true)
    tap.setCached(this)
  }

  override def tapLeft(): TapLRDD[(K, Array[Iterable[_]])] = {
    if(newDeps.isEmpty) computeTapDependencies
    new TapPreCoGroupLRDD[(K, Array[Iterable[_]])](lineageContext, Seq(newDeps.pop()))
      .setCached(this)
  }
}