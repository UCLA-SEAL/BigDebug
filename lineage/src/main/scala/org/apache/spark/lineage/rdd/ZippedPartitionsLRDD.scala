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

import org.apache.spark.lineage.{Lineage, LineageContext}
import org.apache.spark.rdd.{ZippedPartitionsBaseRDD, ZippedPartitionsRDD2, ZippedPartitionsRDD3}

import scala.reflect._

private[spark] abstract class ZippedPartitionsBaseLRDD[V: ClassTag](
    @transient lc: LineageContext,
    var lrdds: Seq[Lineage[_]],
    preservesPartitioning: Boolean = false)
    extends ZippedPartitionsBaseRDD[V](lc.sparkContext, lrdds) with Lineage[V] {

  override def ttag = classTag[V]

  override def lineageContext = lc
}

private[spark] class ZippedPartitionsLRDD2[A: ClassTag, B: ClassTag, V: ClassTag](
    @transient lc: LineageContext,
    f: (Iterator[A], Iterator[B]) => Iterator[V],
    var lrdd1: Lineage[A],
    var lrdd2: Lineage[B],
    preservesPartitioning: Boolean = false)
    extends ZippedPartitionsRDD2[A, B, V](lc.sparkContext, f, lrdd1, lrdd2, preservesPartitioning)
    with Lineage[V] {

  override def ttag = classTag[V]

  override def lineageContext = lc
}

private[spark] class ZippedPartitionsLRDD3[A: ClassTag, B: ClassTag, C: ClassTag, V: ClassTag](
    @transient lc: LineageContext,
    f: (Iterator[A], Iterator[B], Iterator[C]) => Iterator[V],
    var lrdd1: Lineage[A],
    var lrdd2: Lineage[B],
    var lrdd3: Lineage[C],
    preservesPartitioning: Boolean = false)
    extends ZippedPartitionsRDD3[A, B, C, V](lc.sparkContext, f, lrdd1, lrdd2, lrdd3, preservesPartitioning)
    with Lineage[V] {

  override def ttag = classTag[V]

  override def lineageContext = lc
}