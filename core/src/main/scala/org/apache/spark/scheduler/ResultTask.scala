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

import java.nio.ByteBuffer

import java.io._

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
 * A task that sends back the output to the driver application.
 *
 * See [[Task]] for more information.
 *
 * @param stageId id of the stage this task belongs to
 * @param taskBinary broadcasted version of the serialized RDD and the function to apply on each
 *                   partition of the given RDD. Once deserialized, the type should be
 *                   (RDD[T], (TaskContext, Iterator[T]) => U).
 * @param partition partition of the RDD this task is associated with
 * @param locs preferred task execution locations for locality scheduling
 * @param outputId index of the task in this job (a job can launch tasks on only a subset of the
 *                 input RDD's partitions).
 */
private[spark] class ResultTask[T, U](
    stageId: Int,
    taskBinary: Broadcast[Array[Byte]],
    partition: Partition,
    @transient locs: Seq[TaskLocation],
    val outputId: Int)
  extends Task[U](stageId, partition.index) with Serializable {

  @transient private[this] val preferredLocs: Seq[TaskLocation] = {
    if (locs == null) Nil else locs.toSet.toSeq
  }

  override def getDetailedInfo:String = {
    // available?
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    func.toString + " (packed " + taskBinary.value.size + ")"
  }
  override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    scala.Console.println("## runTask ## stageId:" + context.stageId + ", partitionId:" + context.partitionId)

    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)

    metrics = Some(context.taskMetrics)
    try {
      scala.Console.println("## func begin ## stageId:" + context.stageId + ", partitionId:" + context.partitionId)
      scala.Console.println("Current Thread Id [local] : " + Thread.currentThread().getId)
      scala.Console.println("## Thread ## stageId:" + context.stageId + ", partitionId:" + context.partitionId + ", id:" + Thread.currentThread().getId())
      val ret: U = func(context, rdd.iterator(partition, context))
      scala.Console.println("## func end ## stageId:" + context.stageId + ", partitionId:" + context.partitionId)
      ret
    } finally {
      context.markTaskCompleted()
    }
  }

  // This is only callable on the driver side.
  override def preferredLocations: Seq[TaskLocation] = preferredLocs

  override def toString = "ResultTask(" + stageId + ", " + partitionId + ")"
}
