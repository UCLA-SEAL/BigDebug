package org.apache.spark.bdd

import java.util.concurrent.atomic.AtomicLong

import com.thoughtworks.xstream.XStream
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{NotifyActualTaskCompleted, NotifyCrashCulprit, ExceptionNotification}
import org.apache.spark.{TaskContext, TaskContextImpl}

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * Created by ali on 5/9/15.
 * This is worker side class
 */
object CrashCulpritManager {
	var bdconfig_ : BigDebugConfiguration = null
	var tempconfig = new BigDebugConfiguration()
	var crashID: Long = 0L;

	/*def newCrashID(): Long = {
		crashID.synchronized{
			crashID = crashID + 1L
			crashID
		}
	}*/
	var c_id: AtomicLong = new AtomicLong(0L);

	def getcrashId(rddid: Int, taskId: Int): Int = {

			("" + rddid + taskId + c_id.incrementAndGet()).hashCode()

	}

	def bdconfig: BigDebugConfiguration = if (bdconfig_ == null) {
		tempconfig
	} else {
		bdconfig_
	}

	def catchException(crashingRecord: CrashingRecord): Unit = {
		println("(" + crashingRecord.stageID + "," + crashingRecord.taskID + "," + crashingRecord.rddid + ") " + crashingRecord.record + "    Error: " + crashingRecord.exception)
		//ExecutorManager.sendMessage(ExceptionNotification(crashingRecord))
	}

	var currentCrash: Any = null
	var currentCrashSkip: Boolean = true
	private val waitObjects = HashMap[(Int, Int, Int), Object]()
	private val skippedCrashedRecords = mutable.HashMap[(Int, Int, Int), List[Any]]().withDefaultValue(Nil)
	private val resolvedCrashedRecords = mutable.HashMap[(Int, Int, Int), List[CrashingRecord]]().withDefaultValue(Nil)


	def setBigDebugConfiguration(c: BigDebugConfiguration): Unit = {
		bdconfig_ = c
	}

	def getBigDebugConfiguration(): BigDebugConfiguration = {
		bdconfig
	}

	def getCurrentCrashCulprit: Any = {
		currentCrash
	}

	/**
	 *
	 * Pause Eecution at the the subtask level here
	 * Ask for User intervention, if Configuration for instant crash resolution
	 * or Skipp every thing for lazy resolution
	 * Triggered from crash exception handling code in trasnformation
	 *
	 **/


	def setCrash(record: Any, rddid: Int, exception: Exception, context: TaskContext): Any = {
		val crashingRecorId = getcrashId(rddid, context.partitionId())

		val stageID = context.stageId()
		val taskID = context.partitionId()
		//		val currentID = context match {
		//			case _: TaskContextImpl =>
		//				val a = context.asInstanceOf[TaskContextImpl].currentInputId
		//        if(a.length == 0)
		//          (-1,-1)
		//        else
		//          a(0)
		//			case _ =>
		//        (-1, -1)
		//		}
		val currentID = context match {
			case _: TaskContextImpl =>
				val a = context.asInstanceOf[TaskContextImpl].crashCulpritId
				(context.partitionId(), a)
			case _ =>
				(-1, -1L)
		}
		val xstream: XStream = new XStream()
		val str = xstream.toXML(record)
		println("Sending Record " + str)
		currentCrash = str
		if (bdconfig.CRASH_CULPRIT_RESOLUTION == 1 || (bdconfig.CRASH_CULPRIT_RESOLUTION == 2 && skippedCrashedRecords((stageID, taskID, rddid)).length >= bdconfig.CRASH_CUPLRIT_THRESHOLD)) {
			val c_record = CrashingRecord(str, context.stageId(), context.partitionId(), rddid, exception, crashingRecorId, ExecutorManager.GetExecutorId, true, currentID)
			catchException(c_record)
			ExecutorManager.sendMessage(NotifyCrashCulprit(c_record))
			var waitObject: Object = null
			waitObject = waitObjects.getOrElse((stageID, taskID, rddid), null)
			if (waitObject == null) {
				waitObject = new Object()
				waitObjects += (((stageID, taskID, rddid), waitObject))
			}
			waitObject.synchronized {
				waitObject.wait()
				println("Wait Release")
			}
		}
		else {
			currentCrashSkip = true
			val driver = ExecutorManager.GetDriver
			val c_record = CrashingRecord(str, context.stageId(), context.partitionId(), rddid, exception, crashingRecorId, ExecutorManager.GetExecutorId, false, currentID)
			ExecutorManager.sendMessage(NotifyCrashCulprit(c_record))
			//TaskExecutionManager.enrollCrash(c_record)
			skippedCrashedRecords((stageID, taskID, rddid)) ::= record
		}
		println("Returning value: " + currentCrash)
		if (currentCrashSkip) null else xstream.fromXML(currentCrash.toString)
	}


	/**
	 *
	 * Resume Execution at the the subtask level after User intervention
	 * Triggered from Driver Executor Backend
	 *
	 **/
	def resolveCrash(action: Int , c: CrashingRecord) = {
		println("Mod Crash : " + c.record)
		action match {
			case 0 => // Skip record
				currentCrashSkip = true
			case 1 => // Modify Record
				currentCrash = c.record
			case _ =>
		}
		val waitObject = waitObjects.getOrElse((c.stageID, c.taskID,c.rddid), null)
		if (waitObject != null) {
			waitObject.synchronized {
				waitObject.notify()
			}
		} else {
			println("WaitObject is null")
		}
	}


	/**
	 *
	 * Called After the actual task execution done
	 * Batch crash resolution, if configuration permits otherwise skipp all
	 * called in  hasNex
	 *
	 **/

	def lazyCrashCulpritResolution(stageID: Int, taskID: Int, subtaskID: Int): Boolean = {
		//	println("Checking pending resolved crashes")
		if (bdconfig != null)
			bdconfig.CRASH_CULPRIT_RESOLUTION == 2 && resolvedCrashedRecords((stageID, taskID, subtaskID)).length > 0
		else false
	}


	def getResolvedRecord(stageID: Int, taskID: Int, subtaskID: Int): Any = {
		val xstream: XStream = new XStream()
		var str: Any = null
		if (resolvedCrashedRecords((stageID, taskID, subtaskID)).length > 0) {
			str = resolvedCrashedRecords((stageID, taskID, subtaskID))(0).record
			resolvedCrashedRecords((stageID, taskID, subtaskID)) = resolvedCrashedRecords((stageID, taskID, subtaskID)).drop(1)
		}
		println("Setting resolved record (" + stageID + "," + taskID + "," + subtaskID + ") " + str)
		xstream.fromXML(str.toString)
	}


	def resolvePendingCrashes(list: List[CrashingRecord], stageID: Int, taskID: Int, subtaskID: Int) = {
		println("Got resolved record list (" + stageID + "," + taskID + "," + subtaskID + ") " + list.length)
		var list_notNull: List[CrashingRecord] = Nil
		for (str <- list) {
			if (str != null) list_notNull = str :: list_notNull
		}
		resolvedCrashedRecords((stageID, taskID, subtaskID)) = list_notNull
		//TODO: We should match the size of skipped records with resolved records
		var waitObject: Object = null
		waitObjects.synchronized {
			waitObject = waitObjects.getOrElse((stageID, taskID, subtaskID), null)
		}
		if (waitObject != null) {
			waitObject.synchronized {
				waitObject.notify()
			}
		}
	}

	def skipThisTask(stageID: Int, taskID: Int, subtaskID: Int) = {
		println("Releasing Resources from this task" + stageID + " " + taskID + " " + subtaskID)
		resolvedCrashedRecords((stageID, taskID, subtaskID)) = List[CrashingRecord]()
		var waitObject: Object = null
		waitObjects.synchronized {
			waitObject = waitObjects.getOrElse((stageID, taskID, subtaskID), null)
		}
		if (waitObject != null) {
			waitObject.synchronized {
				waitObject.notify()
			}
		}
	}


	def requestLazyResolution(stageID: Int, taskID: Int, subtaskID: Int) {
		if (skippedCrashedRecords((stageID, taskID, subtaskID)).length > 0 && bdconfig.CRASH_CULPRIT_RESOLUTION == 2) {
			var waitObject: Object = null
			waitObjects.synchronized {
				waitObject = waitObjects.getOrElse((stageID, taskID, subtaskID), null)
			}
			if (waitObject == null) {
				waitObject = new Object()
				waitObjects.synchronized {
					waitObjects += (((stageID, taskID, subtaskID), waitObject))
				}
			}
			val driver = ExecutorManager.GetDriver
			ExecutorManager.sendMessage(NotifyActualTaskCompleted(stageID, taskID, subtaskID, true))
			waitObject.synchronized {
				println("Sending Crash resolution request (" + stageID + "," + taskID + "," + subtaskID + ") ")
				waitObject.wait()
			}
			skippedCrashedRecords.remove((stageID, taskID, subtaskID))
		}
	}
}


case class CrashingRecord(record: String, stageID: Int, taskID: Int, rddid: Int, exception: Exception, srnumn: Int, senderId: String = "", blocking: Boolean = false, lineageID: (Int, Long) = (-1, -1L))
