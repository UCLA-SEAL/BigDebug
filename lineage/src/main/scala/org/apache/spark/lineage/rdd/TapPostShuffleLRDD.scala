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
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.{LCacheManager, LineageContext}
import org.apache.spark.util.collection.PrimitiveVector

import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

private[spark]
class TapPostShuffleLRDD[T: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps)
{
  implicit def fromTtoProduct2[T](record: T) = record.asInstanceOf[Product2[T, Int]]

  override def getCachedData = shuffledData.setIsPostShuffleCache()

  private[spark] def unroll(h: RecordId, t: ListBuffer[RecordId]): List[(RecordId, RecordId)] =
    if(t.isEmpty) Nil else (h, t.head) :: unroll(h, t.tail)

  override def materializeRecordInfo: Array[Any] =
    //tContext.currentRecordInfos.flatMap(r => unroll(r._2.head, r._2.tail)).toArray
    tContext.currentRecordInfos.zip(recordInfo1.array).flatMap(r => r._1._2.map(r2 => (r._2, r2))).toArray

  private[spark] def update(value: Int) = (hadValue: Boolean, oldValue: Int) => {
    if (hadValue) value else value
  }

  override def compute(split: Partition, context: TaskContext) = {
    if(tContext == null) {
      tContext = context
    }
    splitId = split.index.toShort

    recordInfo1 = new PrimitiveVector[Int]()

    SparkEnv.get.cacheManager.asInstanceOf[LCacheManager].initMaterialization(this, split)

    firstParent[T].iterator(split, context).map(tap)
  }

  @transient private[spark] var recordInfo1: PrimitiveVector[Int] = null

  override def tap(record: T) = {
    recordInfo1 += record._2
//      (id.toShort, splitId, record._2._2) :: _)
//    tContext.currentRecordInfos.changeValue(
//      record._1._1.hashCode(),
//      new ListBuffer().+=:(id.toShort, splitId, record._2._2),
//    _.+=:((id.toShort, splitId, record._2._2)))
    tContext.currentRecordInfo = record._2

    record._1
  }
}