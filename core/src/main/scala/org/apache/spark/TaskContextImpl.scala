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

package org.apache.spark

import java.util.{Properties, Queue}
import java.util.concurrent.ThreadPoolExecutor

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.lineage.util.ByteBuffer
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.util._

import scala.collection.mutable.{HashMap, Map}

private[spark] class TaskContextImpl(
                                      val stageId: Int,
                                      val partitionId: Int,
                                      override val taskAttemptId: Long,
                                      override val attemptNumber: Int,
                                      override val taskMemoryManager: TaskMemoryManager,
                                      localProperties: Properties,
                                      @transient private val metricsSystem: MetricsSystem,
                                      // The default value is only used in tests.
                                      override val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContext
  with Logging {

  /** List of callback functions to execute when the task completes. */
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  /** List of callback functions to execute when the task fails. */
  @transient private val onFailureCallbacks = new ArrayBuffer[TaskFailureListener]

  // Whether the corresponding task has been killed.
  @volatile private var interrupted: Boolean = false

  // Whether the task has completed.
  @volatile private var completed: Boolean = false

  // Whether the task has failed.
  @volatile private var failed: Boolean = false

  /**
   * ***************************************** Matteo ********************************************
   */
  // (and some Jason)
  
  
  // Used to pipeline records through taps inside the same stage
  @transient var currentInputId: Int = -1
  
  // Jason - Used to track performance for records per RDD(id)
  @transient private var rddTimeMap: Map[Int, Long] = new HashMap()

  // Used to pipeline records through taps inside the same stage
  @transient var currentBuffer: ByteBuffer[Long, Int] = null

  @transient var threadPool: ThreadPoolExecutor = null

  @transient private var bufferPool: Queue[Array[Byte]] = null

  @transient private var bufferPoolLarge: Queue[Array[Byte]] = null
  
  // jteoh: added size fields and setters - these are normally set by Task
  @transient private var bufferPoolSize: Int = _
  @transient private var bufferPoolLargeSize: Int = _

  def setThreadPool(pool: ThreadPoolExecutor): Unit = this.threadPool = pool

  def setBufferPool(pool: Queue[Array[Byte]]): Unit = this.bufferPool = pool

  def setBufferPoolLarge(pool: Queue[Array[Byte]]): Unit = this.bufferPoolLarge = pool

  def getFromBufferPool(): Array[Byte] = {
    val buffer = bufferPool.poll()
    if (buffer == null) {
      return new Array[Byte](64 * 1024 * 128)
    }
    buffer
  }

  def getFromBufferPoolLarge(): Array[Byte] = {
    val buffer = bufferPoolLarge.poll()
    if (buffer == null) {
      return new Array[Byte](64 * 1024 * 1024)
    }
    buffer
  }

  def addToBufferPool(data: Array[Byte]): Unit = bufferPool.add(data)

  def addToBufferPoolLarge(data: Array[Byte]): Unit = bufferPoolLarge.add(data)
  
  // jteoh: added size setters - these are normally set by Task
  def setBufferPoolSize(size: Int) = this.bufferPoolSize = size
  def setBufferPoolLargeSize(size: Int) = this.bufferPoolLargeSize = size
  
  // Jason - KISS for now. Might add more for optimizations later, so
  // no direct access to the underlying map.
  // Key = RDD, value = time taken for last output record, ie how long it took to if rdd.map(udf)
  // yields rdd2, the time it took to generate a record in rdd2 will be stored with rdd as the key.
  // This is because the newly generated RDD's id is not determined until it is actually created,
  // but the UDF provided needs access to a given RDD.
  def updateRDDRecordTime(rddId: Int, timeNanos: Long) = rddTimeMap(rddId) = timeNanos
  
  // Jason - might want to add some sort of default value, but that could also depend on how we
  // want to time our calls.
  def getRddRecordOutputTime(rddId: Int): Long = rddTimeMap(rddId)
  
  // Jason - exposed under assumption that the task context will only contain a linear DAG, which
  // should be the case since any branches/merges should be due to shuffles.
  def getSummedRddRecordTime(): Long = rddTimeMap.valuesIterator.sum
  /**
   * *************************************************************************************
   */


  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    onCompleteCallbacks += listener
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): this.type = {
    onFailureCallbacks += listener
    this
  }

  /** Marks the task as failed and triggers the failure listeners. */
  private[spark] def markTaskFailed(error: Throwable): Unit = {
    // failure callbacks should only be called once
    if (failed) return
    failed = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process failure callbacks in the reverse order of registration
    onFailureCallbacks.reverse.foreach { listener =>
      try {
        listener.onTaskFailure(this, error)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError("Error in TaskFailureListener", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs, Option(error))
    }
  }

  /** Marks the task as completed and triggers the completion listeners. */
  private[spark] def markTaskCompleted(): Unit = {
    completed = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process complete callbacks in the reverse order of registration
    onCompleteCallbacks.reverse.foreach { listener =>
      try {
        listener.onTaskCompletion(this)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError("Error in TaskCompletionListener", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs)
    }
  }

  /** Marks the task for interruption, i.e. cancellation. */
  private[spark] def markInterrupted(): Unit = {
    interrupted = true
  }

  override def isCompleted(): Boolean = completed

  override def isRunningLocally(): Boolean = false

  override def isInterrupted(): Boolean = interrupted

  override def getLocalProperty(key: String): String = localProperties.getProperty(key)

  override def getMetricsSources(sourceName: String): Seq[Source] =
    metricsSystem.getSourcesByName(sourceName)

  private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    taskMetrics.registerAccumulator(a)
  }

}
