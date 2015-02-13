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
import org.apache.spark.lineage.{LCacheManager, LineageContext}
import org.apache.spark.util.collection.PrimitiveVector

private[spark]
class TapHadoopLRDD[K, V](@transient lc: LineageContext, @transient deps: Seq[Dependency[_]])
  extends TapLRDD[(K, V)](lc, deps)
{
  def this(@transient prev: HadoopLRDD[_, _]) =
    this(prev.lineageContext, List(new OneToOneDependency(prev)))

  private[spark] val filePath = firstParent[(K, V)].asInstanceOf[HadoopLRDD[K, V]].getFilePath

  private[spark] def addRecordInfo(key: Int, value: Long) = {
    recordInfo += key -> value
  }

  @transient private[spark] var recordInfo1: PrimitiveVector[Int] = null

  @transient private[spark] var recordInfo2: PrimitiveVector[Long] = null

  override def compute(split: Partition, context: TaskContext) = {
    if(tContext == null) {
      tContext = context
    }
    splitId = split.index.toShort

    recordInfo1 = new PrimitiveVector[Int]()

    recordInfo2 = new PrimitiveVector[Long]()

    SparkEnv.get.cacheManager.asInstanceOf[LCacheManager].initMaterialization(this, split)

    firstParent[(K, V)].iterator(split, context).map(tap)
  }

  override def materializeRecordInfo: Array[Any] = recordInfo1.array.zip(recordInfo2.array)

  override def tap(record: (K, V)) = {
    tContext.currentRecordInfo = newRecordId
    recordInfo1 += tContext.currentRecordInfo
    recordInfo2 += record._1.asInstanceOf[LongWritable].get
    //addRecordInfo(tContext.currentRecordInfo, record._1.asInstanceOf[LongWritable].get)

    record
  }
}
