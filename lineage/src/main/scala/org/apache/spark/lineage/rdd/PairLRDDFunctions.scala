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

import org.apache.spark.Partitioner._
import org.apache.spark.lineage.LAggregator
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.rdd._
import org.apache.spark.serializer.Serializer
import org.apache.spark.{HashPartitioner, Partitioner, SparkException}

import scala.language.implicitConversions
import scala.reflect.ClassTag

private[spark] class PairLRDDFunctions[K, V](self: Lineage[(K, V)])
(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctions[K, V](self)
{
  def lineageContext = self.lineageContext

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  override def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner)
  : Lineage[(K, (Iterable[V], Iterable[W]))]  = {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new CoGroupedLRDD[K](Seq(self, other), partitioner)
    cg.mapValues { case Array(vs, w1s) =>
      (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])
    }
  }

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   * Note that V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   * - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   */
  override def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null): Lineage[(K, C)] = {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    if (keyClass.isArray) {
      if (mapSideCombine) {
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("Default partitioner cannot partition array keys.")
      }
    }
    val aggregator = new LAggregator[K, V, C](
      createCombiner,
      mergeValue,
      mergeCombiners,
      lineageContext.isLineageActive)

    new ShuffledLRDD[K, V, C](self, partitioner)
      .setSerializer(serializer)
      .setAggregator(aggregator)
      .asInstanceOf[ShuffledLRDD[K, V, C]]
      .setMapSideCombine(mapSideCombine)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
   */
  override def reduceByKey(func: (V, V) => V): Lineage[(K, V)] = {
    reduceByKey(defaultPartitioner(self), func)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   */
  override def reduceByKey(partitioner: Partitioner, func: (V, V) => V): Lineage[(K, V)] = {
    combineByKey[V]((v: V) => v, func, func, partitioner)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
   */
  override def reduceByKey(func: (V, V) => V, numPartitions: Int): Lineage[(K, V)] = {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
   */
  override def join[W](other: RDD[(K, W)], partitioner: Partitioner): Lineage[(K, (V, W))] = {
    this.cogroup(other, partitioner).flatMapValues( pair =>
      for (v <- pair._1; w <- pair._2) yield (v, w)
    )
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  override def join[W](other: RDD[(K, W)]): Lineage[(K, (V, W))] = {
    join(other, defaultPartitioner(self, other))
  }

  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  override def mapValues[U](f: V => U): Lineage[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MappedValuesLRDD(self, cleanF)
  }

  /**
   * Pass each value in the key-value pair RDD through a flatMap function without changing the
   * keys; this also retains the original RDD's partitioning.
   */
  override def flatMapValues[U](f: V => TraversableOnce[U]): Lineage[(K, U)] = {
    val cleanF = self.context.clean(f)
    new FlatMappedValuesLRDD(self, cleanF)
  }
}

object PairLRDDFunctions {
  implicit def RDDToLineage(rdd: RDD[_]) = rdd.asInstanceOf[Lineage[_]]
}
