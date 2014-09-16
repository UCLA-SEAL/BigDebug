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

package org.apache.spark.rdd

import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag

private[spark]
class UntapShuffleRDD[ T : ClassTag](sc: SparkContext, deps: Seq[Dependency[_]])
    extends TapRDD[T](sc, deps) {

  override def compute(split: Partition, context: TaskContext) =
    firstParent[T].iterator(split, context).map(untap)

  def untap(record: T) = {
    val tappedRecord = record.asInstanceOf[Product3[_, _, Seq[(Int, Int, Long)]]]
    val id = (firstParent[T].id, splitId, newRecordId)
    recordInfo += ((id, tappedRecord._3))
    println("Untapping (" + tappedRecord._1 + ", " + tappedRecord._2 + ") with id " +
      id + " join with " + tappedRecord._3)
    (tappedRecord._1, tappedRecord._2).asInstanceOf[T]
  }
}