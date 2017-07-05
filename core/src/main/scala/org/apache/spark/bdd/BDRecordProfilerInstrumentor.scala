package org.apache.spark.bdd

import org.apache.spark.executor.ui.ExecutorWebUI
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{BDDMetricTaskDone, BDDMetricTaskCurrentTime, BDDMetricTaskStart}

import scala.collection.mutable.HashMap


/** *Redesign to avoid singleton instance over all the workers **/
object BDRecordProfiler {

	/** Fix it by adding it as a parent to this BDDMETric classs. **/
	var executorUI: ExecutorWebUI = null;
	var bdconfig: BDConfiguration = null


	var rddID: Int = -1
	var recordAvgStd = HashMap[(Int, Int, Int), (Long /*Sum N*N*/ , Long /*Sum N*/ , Int /* n */ , String)]()
	var top10 = HashMap[Int, Array[(String, Long)]]().withDefaultValue(new Array[(String, Long)](10))

	var currentRDDConfiguration = HashMap[(Int, Int), (Boolean /*watchpoint*/ , Boolean /*latency*/ , Int /*RDD ID*/ )]()

	def setBigDebugConfiguration(c: BDConfiguration): Unit = {
		bdconfig = c
	}

	def getBigDebugConfiguration(): BDConfiguration = {
		bdconfig
	}

	def setExecutorUI(exUI: ExecutorWebUI): Unit = {
		executorUI = exUI
	}

	def setCurrentRDDConfig(rddID: Int, taskID: Int, wp: Boolean, latency: Boolean, rdd: Int): Unit = {
		currentRDDConfiguration((rddID, taskID)) = (wp, latency, rdd)
	}

	def getCurrentRDDConfig(rddID: Int, taskID: Int): (Boolean, Boolean, Int) = {

		currentRDDConfiguration.getOrElse((rddID, taskID), (false, false, 0))

	}

	def removeCurrentRDDConfig(rddID: Int, taskID: Int): Unit = {
		currentRDDConfiguration.remove((rddID, taskID))
	}


	def recordDoneNotification(delta: Long, sID: Int, pid: Int, rdd: Int, record: String) {
		//println("reporting delayed record")
		recordAvgStd.synchronized {
			var (sumSQ, sum, n, prevRecord) = recordAvgStd.getOrElse((sID, pid, rdd), (0L, 0L, 0, null))
			sumSQ = sumSQ + (delta * delta)
			sum = sum + delta
			n = n + 1
			val avg = sum.asInstanceOf[Float] / n.asInstanceOf[Float]
			val std = Math.sqrt(((sumSQ.asInstanceOf[Float] / n.asInstanceOf[Float]) - (avg * avg)))
			DebugHelper.log("INFO", "BDDMetric", s"** Current Record(rddID: $sID, taskID: $pid, (Time Spent: $delta, Record: $record **")
			if (prevRecord == null) {
				recordAvgStd((sID, pid, rdd)) = (sumSQ, sum, n, record)
				insertInTop10(rdd, record, delta)
			}
			else {
				if (delta > ((bdconfig.KDEV * std) + avg) && delta > 1) {
					DebugHelper.log("INFO", "BDDMetric", s"** Straggling Record(rddID: $sID, taskID: $pid, (Time Spent: $delta, Record: $record **")
					insertInTop10(rdd, record, delta)
					recordAvgStd((sID, pid, rdd)) = (sumSQ, sum, n, record)
					updateRecordProfileUI(rdd)
				} else {
					recordAvgStd((sID, pid, rdd)) = (sumSQ, sum, n, prevRecord)
				}
			}
		}

	}

	def updateRecordProfileUI(rdd: Int): Unit = {
		if (executorUI != null) {
			executorUI.executorWebSocket.s.updateRecordProfileUI(rdd)
		}
	}

	def updateTaskInfo(tID: Long, sID: Int): Unit = {
		val time = System.currentTimeMillis()
		BDExecutorManager.sendMessage(BDDMetricTaskStart(sID, tID, BDExecutorManager.GetExecutorId, time))
	}


	def updateTaskDone(tID: Long, sID: Int): Unit = {

		val time = System.currentTimeMillis()
		BDExecutorManager.sendMessage(BDDMetricTaskDone(sID, tID, BDExecutorManager.GetExecutorId, time))
	}

	/** *****
	  *
	  * Incorpoorate the task IDs so that there is differnet UI for each task.
	  * Task ID needs to be sent  by the driver through the chart
	  *
	  *
	  */
	def getUiProfileData(rdd: Int): String = {
		var content = top10(rdd).map { a =>
			if (a == null) {
				""
			} else {
				val r = a._1
				val t = a._2/1000000
				s"""{"label": "$r" , "y" : $t }"""
			}
		}

		var (sumSQ, sum, n, prevRecord) = (0L, 0L, 0, null)
		recordAvgStd.synchronized {
			for (k <- recordAvgStd.keySet) {
				if (k._3 == rdd) {

					var (st, s, i, prevRecord) = recordAvgStd.getOrElse((k._1, k._2, rdd), (0L, 0L, 0, null))
					sum = sum + s
					sumSQ = sumSQ + st
					n = n + i
				}
			}
		}
		content = content.dropWhile(x => x.equals(""))
		var str = ""
		if (content.size > 0) {
			str = content.toList.reduce(_ + "," + _)
		}
		str = str.replaceAll(",+", ",")
		var avg = sum.asInstanceOf[Float] / n.asInstanceOf[Float]
		avg = avg/1000000
		val std = Math.sqrt(((sumSQ.asInstanceOf[Float] / n.asInstanceOf[Float]) - (avg * avg)))
		str = "[" + {
			if (str.endsWith(",")) str else str + ","
		} + s"""{"label": "Average", "y":$avg},{"stddev":$std}]"""
		println(top10(rdd).toList)
		str
	}

	def insertInTop10(rdd: Int, record: String, time: Long): Unit = {
		top10.synchronized {
			val arr = top10(rdd)
			if (arr.indexWhere(b => b == null) == -1) {
				val indx = arr.indexWhere(b => if (b != null) b._2 <= time else true)
				if (indx != -1) arr(indx) = (record, time)
			} else {
				val indx = arr.indexWhere(b => b == null)
				arr(indx) = (record, time)
			}
			top10(rdd) = arr
		}
	}

	def getRDDs(): Iterator[Int] = {
		var rdds_list = Set[Int]()
		for (k <- recordAvgStd.keySet) {
			rdds_list += k._3
		}
		rdds_list.toIterator

	}

}

class BDRecordProfilerInstrumentor[T](sid: Int, pid: Int, rddid: Int, bDDIterator: BDDIterator[T]) {

	def wrapClosureForProfiling[U, T](f: T => U): T => U = {
		if (bDDIterator.isRecordLevelLatencyEnabled) {
			val newf: T => U = { x =>
				val stime = System.nanoTime()
				val retval = f(x)
				val etime = System.nanoTime()
				new Thread {
					override def run(): Unit = {
						BDRecordProfiler.recordDoneNotification(etime - stime, sid, pid, rddid, x.toString)
					}
				}.start()
				retval
			}
			newf
		} else {
			f
		}
	}
}

class BDMetric {
	var executionstarttime = 0L
	var executionDonetime = 0L
	var executionSpan = 0L
	var finished = false
}