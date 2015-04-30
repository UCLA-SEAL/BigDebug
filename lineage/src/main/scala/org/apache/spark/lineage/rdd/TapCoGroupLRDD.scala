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
import org.apache.spark.util.collection.{CompactBuffer, PrimitiveKeyOpenHashMap}

import scala.reflect.ClassTag

private[spark]
class TapCoGroupLRDD[T: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps)
{
  @transient private var inputIdStore: PrimitiveKeyOpenHashMap[Int, CompactBuffer[Int]] = null

  override def getCachedData = shuffledData.setIsPostShuffleCache()

  override def initializeStores() = inputIdStore = new PrimitiveKeyOpenHashMap

  override def materializeRecordInfo: Array[Any] =
    inputIdStore.toArray.zipWithIndex.flatMap(
      r1 => r1._1._2.toArray.map(r2 => (r1._2, (r2, r1._1._1))))

  override def tap(record: T) = {
    var trace = CompactBuffer[Int]
    val (key, values) = record.asInstanceOf[(T, Array[Iterable[(_, Int)]])]
    val iters = for(iter <- values) yield({
      iter.map(r => {
        trace += r._2
        r._1
      })
    })
    tContext.currentInputId = newRecordId()
    inputIdStore.update(key.hashCode, trace)

    (key, iters).asInstanceOf[T]
  }
}
