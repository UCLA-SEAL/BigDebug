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

package org.apache.spark.scheduler.cluster

import java.nio.ByteBuffer

import org.apache.spark.TaskState.TaskState
import org.apache.spark.bdd.{CrashingRecord, CapturedRecord, BDConfiguration}
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.ExecutorLossReason
import org.apache.spark.util.SerializableBuffer

import scala.collection.mutable

private[spark] sealed trait CoarseGrainedClusterMessage extends Serializable

private[spark] object CoarseGrainedClusterMessages {

	/**
	 * Message Passing Classes for Bigebug --Tag Bigdebug @ Gulzar 6/16
	 **/
	case class RegisterExecutorDebugger(
		                           executorId: String,
		                           executorRef: RpcEndpointRef,
		                           hostname: String)
		extends CoarseGrainedClusterMessage

	case class TestMessage(str: String) extends CoarseGrainedClusterMessage

	case class RegisterSocketInfo(port: Int, host: String, id: String)
		extends CoarseGrainedClusterMessage

	case class TupleNotificationForDriver(id: String, list: List[(Any, Any, Int, Int)], typ: Int)
		extends CoarseGrainedClusterMessage

	case class SendWatchpointDataToDriver(id: String, list: List[CapturedRecord], subtaskID: Int)
		extends CoarseGrainedClusterMessage

	case class SetExpressionExecutor(impl: Array[Int], class_name: String, rddID: Int)
		extends CoarseGrainedClusterMessage

	case class SendACKCode(i: Int, str: String, execID: String)
		extends CoarseGrainedClusterMessage

	case class RequestIntermediateData() extends CoarseGrainedClusterMessage

	case class ExceptionNotification(crashingRecord: CrashingRecord)
		extends CoarseGrainedClusterMessage

	case class NotifyCrashCulprit(crashingRecord: CrashingRecord)
		extends CoarseGrainedClusterMessage

	case class CrashRecordAction(action: Int /*1 for modify 0 for skip*/ , crashingRecord: CrashingRecord)
		extends CoarseGrainedClusterMessage

	case class ResolveAllCrashes(record: List[CrashingRecord], stageID: Int, taskID: Int, subtaskID: Int)
		extends CoarseGrainedClusterMessage

	case class NotifyActualTaskCompleted(stageID: Int, taskID: Int, subtaskID: Int, taskDone: Boolean)
		extends CoarseGrainedClusterMessage

	case class RegisteredExecutor(bdconf: BDConfiguration) extends CoarseGrainedClusterMessage with RegisterExecutorResponse

	/**
	 * Edits end here --Tag : Bigdebug @Gulzar 6/16
	 **/

	case object RetrieveSparkAppConfig extends CoarseGrainedClusterMessage

	case class SparkAppConfig(
		                         sparkProperties: Seq[(String, String)],
		                         ioEncryptionKey: Option[Array[Byte]])
		extends CoarseGrainedClusterMessage

	case object RetrieveLastAllocatedExecutorId extends CoarseGrainedClusterMessage

	// Driver to executors
	// For breakpoint code, the new code needs to be shipped. --Tag Bigdebug @ Gulzar 07/07
	case class LaunchTask(data: SerializableBuffer , 	codeFixStore: mutable.HashMap[Int,(String , Array[Int])]) extends CoarseGrainedClusterMessage

	case class KillTask(taskId: Long, executor: String, interruptThread: Boolean)
		extends CoarseGrainedClusterMessage

	sealed trait RegisterExecutorResponse


	case class RegisterExecutorFailed(message: String) extends CoarseGrainedClusterMessage
	with RegisterExecutorResponse

	// Executors to driver
	case class RegisterExecutor(
		                           executorId: String,
		                           executorRef: RpcEndpointRef,
		                           hostname: String,
		                           cores: Int,
		                           logUrls: Map[String, String])
		extends CoarseGrainedClusterMessage

	case class StatusUpdate(executorId: String, taskId: Long, state: TaskState,
	                        data: SerializableBuffer) extends CoarseGrainedClusterMessage

	object StatusUpdate {
		/** Alternate factory method that takes a ByteBuffer directly for the data field */
		def apply(executorId: String, taskId: Long, state: TaskState, data: ByteBuffer)
		: StatusUpdate = {
			StatusUpdate(executorId, taskId, state, new SerializableBuffer(data))
		}
	}

	// Internal messages in driver
	case object ReviveOffers extends CoarseGrainedClusterMessage

	case object StopDriver extends CoarseGrainedClusterMessage

	case object StopExecutor extends CoarseGrainedClusterMessage

	case object StopExecutors extends CoarseGrainedClusterMessage

	case class RemoveExecutor(executorId: String, reason: ExecutorLossReason)
		extends CoarseGrainedClusterMessage

	case class SetupDriver(driver: RpcEndpointRef) extends CoarseGrainedClusterMessage

	// Exchanged between the driver and the AM in Yarn client mode
	case class AddWebUIFilter(
		                         filterName: String, filterParams: Map[String, String], proxyBase: String)
		extends CoarseGrainedClusterMessage

	// Messages exchanged between the driver and the cluster manager for executor allocation
	// In Yarn mode, these are exchanged between the driver and the AM

	case class RegisterClusterManager(am: RpcEndpointRef) extends CoarseGrainedClusterMessage

	// Request executors by specifying the new total number of executors desired
	// This includes executors already pending or running
	case class RequestExecutors(
		                           requestedTotal: Int,
		                           localityAwareTasks: Int,
		                           hostToLocalTaskCount: Map[String, Int])
		extends CoarseGrainedClusterMessage

	// Check if an executor was force-killed but for a reason unrelated to the running tasks.
	// This could be the case if the executor is preempted, for instance.
	case class GetExecutorLossReason(executorId: String) extends CoarseGrainedClusterMessage

	case class KillExecutors(executorIds: Seq[String]) extends CoarseGrainedClusterMessage

	// Used internally by executors to shut themselves down.
	case object Shutdown extends CoarseGrainedClusterMessage

}
