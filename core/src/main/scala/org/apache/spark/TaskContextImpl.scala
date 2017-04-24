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

import java.util.Queue
import java.util.concurrent.ThreadPoolExecutor

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.lineage.util.ByteBuffer
import org.apache.spark.util.{TaskCompletionListener, TaskCompletionListenerException}

import scala.collection.mutable.ArrayBuffer

private[spark] class TaskContextImpl(val stageId: Int,
    val partitionId: Int,
    val attemptId: Long,
    val runningLocally: Boolean = false,
    val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContext
  with Logging {

  // List of callback functions to execute when the task completes.
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  // Whether the corresponding task has been killed.
  @volatile private var interrupted: Boolean = false

  // Whether the task has completed.
  @volatile private var completed: Boolean = false

  /** Matteo *************************************************************************************/
  // Used to pipeline records through taps inside the same stage
  @transient var currentInputId: Int = -1

  // Used to mantains input ids for crash culprit
 // @transient var currentInputId: List[(Int, Any)] = Nil

  // Used to pipeline records through taps inside the same stage
  @transient var currentBuffer: ByteBuffer[Long, Int] = null

  @transient var threadPool: ThreadPoolExecutor = null

  @transient private var bufferPool: Queue[Array[Byte]] = null

  @transient private var bufferPoolLarge: Queue[Array[Byte]] = null

  def setThreadPool(pool: ThreadPoolExecutor) = this.threadPool = pool

  def setBufferPool(pool: Queue[Array[Byte]]) = this.bufferPool = pool

  def setBufferPoolLarge(pool: Queue[Array[Byte]]) = this.bufferPoolLarge = pool

  def getFromBufferPool(): Array[Byte] = {
    val buffer = bufferPool.poll()
    if(buffer == null) {
      return new Array[Byte](64 * 1024 * 128)
    }
    buffer
  }

  def getFromBufferPoolLarge(): Array[Byte] = {
    val buffer = bufferPoolLarge.poll()
    if(buffer == null) {
      return new Array[Byte](64 * 1024 * 1024)
    }
    buffer
  }

  def addToBufferPool(data: Array[Byte]) = bufferPool.add(data)

  def addToBufferPoolLarge(data: Array[Byte]) = bufferPoolLarge.add(data)

  /***********************************************************************************************/

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    onCompleteCallbacks += listener
    this
  }

  override def addTaskCompletionListener(f: TaskContext => Unit): this.type = {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f(context)
    }
    this
  }

  @deprecated("use addTaskCompletionListener", "1.1.0")
  override def addOnCompleteCallback(f: () => Unit) {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f()
    }
  }

  /** Marks the task as completed and triggers the listeners. */
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

  override def isCompleted: Boolean = completed

  override def isRunningLocally: Boolean = runningLocally

  override def isInterrupted: Boolean = interrupted
}

