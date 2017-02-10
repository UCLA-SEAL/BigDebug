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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.OrderedRDDFunctions
import org.apache.spark.{Partitioner, RangePartitioner}

import scala.reflect.ClassTag

/**
 * Extra functions available on RDDs of (key, value) pairs where the key is sortable through
 * an implicit conversion. Import `org.apache.spark.SparkContext._` at the top of your program to
 * use these functions. They will work with any key type `K` that has an implicit `Ordering[K]` in
 * scope.  Ordering objects already exist for all of the standard primitive types.  Users can also
 * define their own orderings for custom types, or to override the default ordering.  The implicit
 * ordering that is in the closest scope will be used.
 *
 * {{{
 *   import org.apache.spark.SparkContext._
 *
 *   val rdd: RDD[(String, Int)] = ...
 *   implicit val caseInsensitiveOrdering = new Ordering[String] {
 *     override def compare(a: String, b: String) = a.toLowerCase.compare(b.toLowerCase)
 *   }
 *
 *   // Sort by key, using the above case insensitive ordering.
 *   rdd.sortByKey()
 * }}}
 */
class OrderedLRDDFunctions[K : Ordering : ClassTag,
    V: ClassTag,
    P <: Product2[K, V] : ClassTag] @DeveloperApi() (
    self: Lineage[P])
  extends OrderedRDDFunctions[K, V, P](self) {

  private val ordering = implicitly[Ordering[K]]

  /**
   * Sort the RDD by key, so that each partition contains a sorted range of the elements. Calling
   * `collect` or `save` on the resulting RDD will return or output an ordered list of records
   * (in the `save` case, they will be written to multiple `part-X` files in the filesystem, in
   * order of the keys).
   */
  // TODO: this currently doesn't work on P other than Tuple2!
  override def sortByKey(ascending: Boolean = true, numPartitions: Int = self.partitions.size)
      : Lineage[(K, V)] = {
    val part = new RangePartitioner(numPartitions, self, ascending)
    new ShuffledLRDD[K, V, V](self, part)
      .setKeyOrdering(if (ascending) ordering else ordering.reverse)
  }

  /**
   * Repartition the RDD according to the given partitioner and, within each resulting partition,
   * sort records by their keys.
   *
   * This is more efficient than calling `repartition` and then sorting within each partition
   * because it can push the sorting down into the shuffle machinery.
   */
  override def repartitionAndSortWithinPartitions(partitioner: Partitioner): Lineage[(K, V)] =
    new ShuffledLRDD[K, V, V](self, partitioner).setKeyOrdering(ordering)
}
