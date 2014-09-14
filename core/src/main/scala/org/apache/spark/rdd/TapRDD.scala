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

import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.spark.{Dependency, SparkContext, Partition, TaskContext}

private[spark]
class TapRDD[T: ClassTag](sc: SparkContext, deps: Seq[Dependency[_]], where: String) extends RDD[T](sc, deps) {

  private val nextRecord = new AtomicLong(0)

  private def newRecordId = nextRecord.getAndIncrement

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[T].iterator(split, context).map(tap)

  def tap(record: T) = {
    val id = newRecordId
    println("Tapping " + where + " " + record)
    (record, id).asInstanceOf[T]
  }
}