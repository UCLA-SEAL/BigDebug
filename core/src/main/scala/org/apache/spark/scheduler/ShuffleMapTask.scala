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

package org.apache.spark.scheduler

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.util.Properties

import scala.language.existentials
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.lineage.LineageManager
import org.apache.spark.lineage.ignite.{AggregateLatencyStats, IgniteCacheAggregateStatsRepo}
import org.apache.spark.lineage.rdd.{Lineage, PreShuffleLatencyStatsTap, TapPreShuffleLRDD}
import org.apache.spark.lineage.util.CountAndLatencyMeasuringIterator
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.ShuffleWriter

/**
 * A ShuffleMapTask divides the elements of an RDD into multiple buckets (based on a partitioner
 * specified in the ShuffleDependency).
 *
 * See [[org.apache.spark.scheduler.Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param taskBinary broadcast version of the RDD and the ShuffleDependency. Once deserialized,
 *                   the type should be (RDD[_], ShuffleDependency[_, _, _]).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param metrics a `TaskMetrics` that is created at driver side and sent to executor side.
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 */
private[spark] class ShuffleMapTask(
    stageId: Int,
    stageAttemptId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient private var locs: Seq[TaskLocation],
    metrics: TaskMetrics,
    localProperties: Properties,
    jobId: Option[Int] = None,
    appId: Option[String] = None,
    appAttemptId: Option[String] = None)
  extends Task[MapStatus](stageId, stageAttemptId, partition.index, metrics, localProperties, jobId,
    appId, appAttemptId)
  with Logging {

  /** A constructor used only in test suites. This does not require passing in an RDD. */
  def this(partitionId: Int) {
    this(0, 0, null, new Partition { override def index: Int = 0 }, null, null, new Properties)
  }

  @transient private val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def runTask(context: TaskContext): MapStatus = {
    // Deserialize the RDD using the broadcast variable.
    val threadMXBean = ManagementFactory.getThreadMXBean
    val deserializeStartTime = System.currentTimeMillis()
    val deserializeStartCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime
    } else 0L
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, dep) = ser.deserialize[(RDD[_], ShuffleDependency[_, _, _])](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime
    _executorDeserializeCpuTime = if (threadMXBean.isCurrentThreadCpuTimeSupported) {
      threadMXBean.getCurrentThreadCpuTime - deserializeStartCpuTime
    } else 0L

    var writer: ShuffleWriter[Any, Any] = null
    var trackingInputIterator: CountAndLatencyMeasuringIterator[Product2[Any,Any]] = null
    var totalLatency: Long = -1 // although shuffle write metrics has write time, it's not clear
    // how that maps to the data itself (eg across different writers)
    
    try {
      val manager = SparkEnv.get.shuffleManager
      writer = manager.getWriter[Any, Any](dep.shuffleHandle, partitionId, context)
      // jteoh: wrap input iterator to count number of inputs. Also measure time to get/compute
      // the iterator and write output, including stopping the writer (because SortShuffleWriter
      // appears to increment write time metrics within the stop method).
      Lineage.measureTimeWithCallback({
        val records = rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]]
        trackingInputIterator = new CountAndLatencyMeasuringIterator(records)
        writer.write(trackingInputIterator, rdd.isLineageActive) // Youfu
        // The reduce size requires a certain amount of free heap memory in order to work properly.
        // If freeMemory is not enough, we call the garbage collector
        // Matteo
        if(Runtime.getRuntime.freeMemory() < 9000000000L) {
          System.gc()
        }
        writer.stop(success = true).get
      }, totalLatency = _)
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    } finally {
      // jteoh: Before finalizing task cache, compute latency stats for this partition.
      rdd match {
        case pre: PreShuffleLatencyStatsTap[_] =>
          val inputCount = trackingInputIterator.count // NOT USED. oops.
          val outputCountUnused = context.taskMetrics().shuffleWriteMetrics.recordsWritten
          val shuffleLatency = totalLatency - trackingInputIterator.latency
          // assign for upload later.
          val stats = AggregateLatencyStats(inputCount, outputCountUnused, shuffleLatency)
          pre.setLatencyStats(stats)
          log.info(s"Latency stats for $rdd set to $stats")
        case _ =>
          log.info(s"Latency stats not applicable for $rdd")
      }
      
      LineageManager.finalizeTaskCache(rdd, partition.index, context, SparkEnv.get.blockManager,
        appId=appId) // Added by Matteo
    }
  }

  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString: String = "ShuffleMapTask(%d, %d)".format(stageId, partitionId)
}
