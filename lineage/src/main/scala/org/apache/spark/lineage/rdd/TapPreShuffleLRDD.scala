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

import org.apache.spark.lineage.{NewtWrapper, LCacheManager, LineageContext}
import org.apache.spark.util.collection.ExternalAppendOnlyMap
import org.apache.spark.{Dependency, Partition, SparkEnv, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.util.Random

private[spark]
class TapPreShuffleLRDD[T <: Product2[_, _]: ClassTag](
    @transient lc: LineageContext, @transient deps: Seq[Dependency[_]]
  ) extends TapLRDD[T](lc, deps) {

  override def getCachedData = shuffledData.setIsPreShuffleCache()

  //@transient var tmp: AppendOnlyMap[Any, Set[(Short, Int)]] = null
  @transient var tmp: ExternalAppendOnlyMap[Any, (Short, Int), Set[(Short, Int)]] = null

  override def materializeRecordInfo: Array[Any] = tmp.zipWithIndex.flatMap(r => {
      r._1._2.map(r2 => ((tContext.stageId.toShort, splitId, r._2), r2))
    }).toArray.asInstanceOf[Array[Any]]

  override def compute(split: Partition, context: TaskContext) = {
    if(tContext == null) {
      tContext = context
    }
    splitId = split.index.toShort

    recordInfo = new ArrayBuffer[(Any, Any)]()

    //TODO Ksh
    //Using Random Int to avoid same table names
    val newtId:Int = splitId + Random.nextInt(Integer.MAX_VALUE);
    newt = new NewtWrapper(newtId)

    SparkEnv.get.cacheManager.asInstanceOf[LCacheManager].initMaterialization(this, split)

    def mergeValues(c: Set[(Short, Int)], e: (Short, Int)):  Set[(Short, Int)] = c + e

    tmp = new ExternalAppendOnlyMap[Any , (Short, Int), Set[(Short, Int)]]((e: (Short, Int)) => Set(e), mergeValues, (c1: Set[(Short, Int)], c2: Set[(Short, Int)]) => c1 ++ c2)

    firstParent[T].iterator(split, context).map(tap)
  }

  override def cleanTable = {
    tmp = null
  }

  private[spark] def update(value: (Short, Int)) = (hadValue: Boolean, oldValue: Set[(Short, Int)]) => {
    if (hadValue) oldValue + value else Set(value)
  }

  override def tap(record: T) = {
    //tmp.changeValue(record._1, update(tContext.currentRecordInfo))
    tmp.insert(record._1, tContext.currentRecordInfo)

    //TODO Ksh
    newt.add(record._1.toString(),List(tContext.currentRecordInfo.toString()))

    record
  }

  //TODO Ksh
  override def commitNewt(): Unit =
  {
    newt.commit()
  }
}
