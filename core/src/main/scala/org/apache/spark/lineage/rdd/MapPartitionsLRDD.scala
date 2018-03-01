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

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.MapPartitionsRDD

import scala.reflect._

/** Lineage-based extension of MapPartitionsRDD using a function that accepts the current
 * RDD id for the purposes of measuring time/performance.
 */
class MapPartitionsLRDD[U: ClassTag, T: ClassTag](prev: Lineage[T],
    f: (TaskContext, Int, Iterator[T], Int) => Iterator[U],  // (TaskContext, partition index,
                                                              // iterator, rddId)
    preservesPartitioning: Boolean = false)
  // the parent function argument (TC, Int, Iter)=>Iter isn't used because we override compute
  extends MapPartitionsRDD[U, T](prev,null,preservesPartitioning) with Lineage[U] {
  
  override def lineageContext = prev.lineageContext

  override def ttag = classTag[U]
  
  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context), this.id)
}
