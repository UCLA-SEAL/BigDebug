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

package org.apache.spark.lineage

import org.apache.spark.rdd.RDD
import org.apache.spark.storage._
import org.apache.spark.{CacheManager, Partition, TaskContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Spark class responsible for passing RDDs partition contents to the BlockManager and making
 * sure a node doesn't load two copies of an RDD at once.
 */
private[spark] class LCacheManager(blockManager: BlockManager) extends CacheManager(blockManager) {

  private var underMaterialization = new mutable.HashSet[(RDD[_], Int, StorageLevel)]

  def initMaterialization[T](
      rdd: RDD[T], partition: Partition, level: StorageLevel = StorageLevel.MEMORY_ONLY
    ) = underMaterialization += ((rdd.asInstanceOf[RDD[T]], partition.index, level))

  def materialize(
      split: Int,
      context: TaskContext,
      effectiveStorageLevel: Option[StorageLevel] = Some(StorageLevel.MEMORY_ONLY)) = {
    val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    underMaterialization.filter(r => r._2 == split).foreach(table => {
      val key = RDDBlockId(table._1.id, split)
      val arr = table._1.getRecordInfos.toArray.asInstanceOf[Array[Any]]
      try {
        updatedBlocks ++=
          blockManager.putArray(key, arr, table._3, true, effectiveStorageLevel)
        logInfo(s"Trying to materialize Block $key")
      } finally {
        //TODO Ksh
        table._1.commitNewt();
        underMaterialization.remove(table)
        logInfo(s"Block $key materialized")
      }
    })
    val metrics = context.taskMetrics
    val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
    metrics.updatedBlocks = Some(lastUpdatedBlocks ++ updatedBlocks.toSeq)
  }

  override def finalizeTaskCache(rdd: RDD[_], split: Int, context: TaskContext) =
    materialize(split, context)
}
