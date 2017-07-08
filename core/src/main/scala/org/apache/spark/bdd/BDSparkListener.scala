package org.apache.spark.bdd

import org.apache.spark.internal.Logging
import org.apache.spark.lineage.LineageContext
import org.apache.spark.scheduler._
import org.apache.spark.ui.debugger.SocketRunner

/**
 * Created by ali on 7/7/17.
 */
class BDSparkListener extends SparkListener with Logging {
	/*val sc = lc.sparkContext
	val conf = sc.conf
	val bdconf = conf.getBigDebugConfiguration()
*/
	def getMethodName(): String = {
		Thread.currentThread().getStackTrace()(2).getMethodName
	}

	override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
		println(getMethodName())

	}

	override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
		println(getMethodName())
	}

	override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
		println(getMethodName())
		/** Latency alert task done notification -- Tag bigdebug @ Gulzar 06/23 */
		val taskinfo = taskStart.taskInfo
		BDHandlerDriverSide.SetTaskMetricInfo(taskStart.stageId, taskinfo.id, taskinfo.executorId, taskinfo.finishTime)
	}

	override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
		println(getMethodName())
	}

	override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
		println(getMethodName())
		/** Latency alert task done notification -- Tag bigdebug @ Gulzar 06/23 */
		val taskinfo = taskEnd.taskInfo
		BDHandlerDriverSide.SetTaskMetricInfo(taskEnd.stageId, taskinfo.id, taskinfo.executorId, taskinfo.finishTime)
	}

	override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
		println(getMethodName())
	}

	override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
		println(getMethodName())
	}

	override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
		println(getMethodName())
	}

	override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
		println(getMethodName())
	}

	override def onBlockManagerRemoved(
		                                  blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
		println(getMethodName())
	}

	override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
		println(getMethodName())
	}

	override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {

		// Initiating WebSockets and attaching Debugger tabs and handlers -- Tag bigdebug @ Gulzar 06/20
		val driverWebSocket = new SocketRunner(BDHandlerDriverSide.getWebSocketPort(), BDHandlerDriverSide.sparkConf.getBigDebugConfiguration())
		BDHandlerDriverSide.setAndInitiateWebSocket(driverWebSocket)
		println(getMethodName())

	}

	override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
		println(getMethodName())
	}

	override def onExecutorMetricsUpdate(
		                                    executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
		println(getMethodName())
	}

	override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
		println(getMethodName())
	}

	override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
		println(getMethodName())
	}

	override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
		println(getMethodName())
	}

	override def onOtherEvent(event: SparkListenerEvent): Unit = {
		println(getMethodName())
	}
}
