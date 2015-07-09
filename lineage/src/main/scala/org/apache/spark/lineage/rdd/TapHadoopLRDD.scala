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

import org.apache.hadoop.io.LongWritable
import org.apache.spark._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.util.LongIntByteBuffer

private[spark]
class TapHadoopLRDD[K, V](@transient lc: LineageContext, @transient deps: Seq[Dependency[_]])
  extends TapLRDD[(K, V)](lc, deps) {

  def this(@transient prev: HadoopLRDD[_, _]) =
    this(prev.lineageContext, List(new OneToOneDependency(prev)))

  @transient private var buffer: LongIntByteBuffer = _

  override def materializeBuffer: Array[Any] = buffer.iterator.toArray

  override def initializeBuffer = buffer = new LongIntByteBuffer(tContext.getFromBufferPool())

  override def compute(split: Partition, context: TaskContext) = {
    if(tContext == null) {
      tContext = context.asInstanceOf[TaskContextImpl]
    }
    splitId = split.index.toShort
    nextRecord = -1

    //initializeBuffer()

    //SparkEnv.get.cacheManager.asInstanceOf[LCacheManager].initMaterialization(this, split, context)

    firstParent.iterator(split, context).map(tap)
  }

  override def releaseBuffer() = {
    buffer.clear()
    tContext.addToBufferPool(buffer.getData)
  }

  override def tap(record: (K, V)) = {
    tContext.currentInputId = newRecordId() :: record._1.asInstanceOf[LongWritable].get :: Nil
      //  buffer.put(record._1.asInstanceOf[LongWritable].get, nextRecord)
    record
  }
}
