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

import java.io.{PrintWriter, StringWriter}

import org.apache.spark.{Latency, _}
import org.apache.spark.lineage.perfdebug.lineageV2.LineageRecordsStorage
import org.apache.spark.lineage.perfdebug.perftrace.AggregateStatsStorage
import org.apache.spark.lineage.rdd.{LatencyStatsTap, Lineage}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage._

import scala.collection.mutable.HashSet
import scala.language.implicitConversions


/**
 * Spark class responsible for passing RDDs partition contents to the BlockManager and making
 * sure a node doesn't load two copies of an RDD at once.
 */

object LineageManager{

  private var underMaterialization = new HashSet[(RDD[_], Int, Int, StorageLevel)]
  private var blockManager: BlockManager = SparkEnv.get.blockManager

  def initMaterialization[T](
      rdd: RDD[T],
      partition: Partition,
      context: TaskContext,
      level: StorageLevel = StorageLevel.MEMORY_AND_DISK
    ): Unit = underMaterialization.synchronized {
      underMaterialization += ((rdd.asInstanceOf[RDD[T]], partition.index, context.stageId, level))
  }

  def materialize(
      split: Int,
      context: TaskContext,
      effectiveStorageLevel: Option[StorageLevel],
      appId: Option[String]): Unit = {
//    val updatedBlocks = new ArrayBuffer[(BlockId, BlockStatus)]
    val toMaterialize = underMaterialization.synchronized {
      val rdds = underMaterialization.filter(r => (r._2 == split) && (r._3 == context.stageId))
      rdds.foreach(underMaterialization.remove(_))
      rdds
    }

    toMaterialize.foreach(tap => {
      val t = new Runnable() {
        override def run() {
          val rdd = tap._1.asInstanceOf[RDD[_]]
          // val key = RDDBlockId(rdd.id, split) // jteoh: no longer used.
          val linMgrStr = s"$rdd partition $split/${rdd.getNumPartitions}"
          try {
            val arr = materializeRDDBuffer(rdd, linMgrStr)
            uploadLineage(appId, rdd, linMgrStr, arr)
          } catch {
            case e: Exception => {
              println(s"Error storing data for $linMgrStr")
              e.printStackTrace()
              val sw = new StringWriter
              e.printStackTrace(new PrintWriter(sw))
              println(s"Exception for $linMgrStr: \n${sw.toString}")
            }
          } finally {
            tap._1.releaseBuffer()
          }
        }
      }

      context.asInstanceOf[TaskContextImpl].threadPool.execute(t)
    })

//    val metrics = context.taskMetrics
//    val lastUpdatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
//    metrics.updatedBlocks = Some(lastUpdatedBlocks ++ updatedBlocks.toSeq)
  }
  
  private def perfConf = SparkEnv.getPerfConf
  
  private def materializeRDDBuffer(rdd: RDD[_], linMgrStr: String): Array[Any] = {
    if(perfConf.materializeBuffers) {
      debugPrintTimeCallback(
        rdd.materializeBuffer,
        time => println(s"$linMgrStr: Took $time ms to materialize buffer")
      )
    } else {
      println("Warning: RDD Lineage buffer materialization disabled as per perf debug conf")
      Array.empty
    }
  }
  
  private def uploadLineage(appId: Option[String],
                            rdd: RDD[_],
                            linMgrStr: String,
                            arr: Array[Any])  = {
    if(perfConf.uploadLineage) {
      debugPrint(s"$linMgrStr: Attempting to store ${arr.length} entries in record storage")
      // jteoh: remove block manager since we don't store there anymore
      // blockManager.putIterator(key, arr.toIterator, tap._4, true)
  
      val appIdValue = appId.get
      val uploadData = if(perfConf.uploadLineageRecordsLimit > 0) {
        arr.take(perfConf.uploadLineageRecordsLimit)
      } else {
        arr
      }
      
      debugPrintTimeCallback(
        LineageRecordsStorage.getInstance().store(appIdValue, rdd, uploadData),
        time => println(s"$linMgrStr: Storing lineage data took $time ms")
      )
      rdd match {
        case aggStatsTap: LatencyStatsTap[_] =>
          debugPrint(s"$linMgrStr: Attempting to store aggregate stats")
          debugPrintTimeCallback(
            AggregateStatsStorage.getInstance().saveAggStats(appIdValue, aggStatsTap),
            time => println(s"$linMgrStr: Storing aggregate stats took $time ms")
          )
        case _ =>
      }
    } else {
      println("Warning: Upload to ignite is currently disabled by perf debug configuration")
    }
    
  }
  
  def finalizeTaskCache(
      rdd: RDD[_],
      split: Int,
      context: TaskContext,
      _blockManager: BlockManager,
      effectiveStorageLevel: Option[StorageLevel] = Some(StorageLevel.MEMORY_AND_DISK),
      appId: Option[String] = None): Unit = {
    blockManager = _blockManager
    materialize(split, context, effectiveStorageLevel, appId)
  }
  
  val PRINT_DEBUG = true
  private def debugPrintTimeCallback[R](block: => R, fn: Latency => Unit): R = {
    if(PRINT_DEBUG) {
      Lineage.measureTimeWithCallback(block, fn)
    } else {
      block
    }
  }
  
  private def debugPrint(str: String): Unit = {
    if(PRINT_DEBUG) {
      println(str)
    }
  }
  
  
}
