package org.apache.spark.bdd

import java.io._

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.scheduler.Stage
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{CrashRecordAction, ResolveAllCrashes, RequestIntermediateData, SetExpressionExecutor}
import org.apache.spark.ui.debugger.DebuggerPageUtils
import org.apache.spark.ui.scope.RDDOperationGraph
import org.apache.spark.ui.{SparkUI, UIUtils}

import scala.collection.mutable
import scala.collection.mutable._
import scala.xml.Node

/**
 * TaskExecution Manager is Driver-side debugging instance of BigDebug 
 */


object TaskExecutionManager {

	private val executorActor = new HashMap[String, RpcEndpointRef]
	private val executorUI = HashMap[(String), (String, Int)]()
	private var resultOutput = List[(Any, Any, Int, Int)]()

	/** Executor Watchpoint Request Thread configuration */
	var latency: Int = 3000

	var sparkUI: SparkUI = null;
	@transient
	@volatile var requestThreadEnabled: Boolean = true
	var requestThreadstatus = false
	private val requestThread = new Thread(new Runnable {
		def run() {
			if (sparkContext.lc.getBigDebugConfiguration().STRAGGLER_PERIODIC_REQUEST == false)
				return
			while (requestThreadEnabled) {
				/// requestData()
				//  requestExecutorsLocalTime()
				Thread.sleep(latency) // Find any alternative to thread.sleep like Wait-notify**
				//printStragglers()


			// Fix this::	getUiProfileData()
			}
		}
	})


	/**
	 * BigDebugConfiguration
	 **/
	def bigdebugConfiguration = sparkContext.lc.getBigDebugConfiguration()


	/** *Get the executor UI store */
	def enrollExecutorUI(id: String, host: String, port: Int): Unit = {
		executorUI(id) = (host, port)
	}

	/** set Spark UI to get accesss to sockets. Move to parents when this class is redesigned to non-singleton **/
	def setSparkUI(ui: SparkUI): Unit = {
		sparkUI = ui;
	}

	/**
	 *
	 * Predicate function Definition for initial compilation
	 * *** No Need to do this
	 *
	 **/

	private var expression = "\ndef function(value:Any) : Boolean = {" +
		"\n return true \n" +
		"\n}"

	private var stageReset: Int = -1
	private val watchpointed_data = mutable.HashMap[(Int, Int), List[CapturedRecord]]().withDefaultValue(Nil)
	private val taskCounter = mutable.HashMap[(Int, Int), Int]().withDefaultValue(0)
	private val watchpointed_rdds = mutable.HashMap[Int, WatchPointLRDD[_, _]]()

	// NOTE: The purpose of this function is to assign an unique id for each subtask.
	def TaskCounter(stageID: Int, taskID: Int): Int = {
		taskCounter.synchronized {
			val subtaskId = taskCounter.getOrElse((stageID, taskID), -1) + 1
			taskCounter.update((stageID, taskID), subtaskId)
			subtaskId
		}
	}


	def dumpWatchpointData(): Unit = {
		watchpointed_data.synchronized {
			watchpointed_data.foreach {
				keyVal => {
					println(keyVal._1 + ":" + keyVal._2)
				}
			}
		}
	}

	/**
	 *
	 * Get RDD Instance form id
	 **/


	def getRDDFromId(rddID: Int): RDD[_] = {
		var rdd = topRDD

		if (rdd == null) {
			return null
		}


		if (rdd.id == rddID) {
			return topRDD
		}
		var break = true
		while (!rdd.dependencies.isEmpty && break) {
			rdd = rdd.dependencies.head.rdd
			if (rdd.id == rddID)
				break = false

		}
		if (break == false) {
			return rdd
		} else {
			null
		}
	}

	/**
	 * Registering Executor
	 **/
	def registerExecutor(id: String, ex: RpcEndpointRef): Unit = {
		executorActor(id) = ex
	}


	/** Handling the topmost RDD */
	var topRDD: RDD[_] = null
	var currenJobID = -1
	var disableTopRDDset = false;

	def setTopRDD(rdd: RDD[_], id: Int): Unit = {
		if (!disableTopRDDset) {
			topRDD = rdd
			currenJobID = id
		}
		// println("Job ID:" + id)
	}

	def init(): Unit = {
		getExpression
	}

	def getExpression: String = {
		expression.synchronized {
			expression
		}
	}

	def setExpression(str: String): Unit = {
		expression.synchronized {
			expression = str
		}
	}


	def setExpression(str: String, rddID: Int): Unit = {
		// println(str)
		var rdd = topRDD
		var break = true
		while (!rdd.dependencies.isEmpty && break) {
			rdd = rdd.dependencies.head.rdd
			if (rdd.id == rddID)
				break = false

		}
		if (break == false) {
			rdd match {
				case _: WatchPointLRDD[_, _] =>
					println("Set Exppression: " + rdd.toString())
					broadcastExpression(
						rdd.asInstanceOf[WatchPointLRDD[Any, Any]].reCompileFilter(str), rddID
					)

				case _ =>
					println("Not a watchPoint RDD")
			}
		}

	}

	def readPredicateClass(class_name: String): List[Int] = {
		var in = None: Option[FileInputStream]
		var filedata = List[Int]()
		try {
			in = Some(new FileInputStream("/tmp/" + class_name + ".class"))
			var c = 0
			var done = true
			while (done) {
				c = in.get.read
				if (c == -1) {
					done = false
				} else {
					filedata = filedata :+ c

				}
			}
		} catch {
			case e: IOException => e.printStackTrace
		} finally {
			//  println("entered finally ...")
			if (in.isDefined) in.get.close
		}
		return filedata
	}

	def removeExecutor(execID: String): Unit = {
		executorActor.remove(execID)
		DebugHelper.log("INFO", "TaskExecutionManager", s"The actor $execID has been removed.")
	}

	//  def broadcastExpression(): Unit = {
	//    val list = readPredicateClass(predicateClass.getClass().getName)
	//    for ((key, value) <- executorActor) value ! SetExpressionExecutor(list, predicateClass.getClass().getName)
	//
	//  }


	def broadcastExpression(classname: String, rddID: Int): Unit = {
		DebugHelper.log("INFO", "TaskExecutionManager", s"Sending filter")
		val list = readPredicateClass(classname)
		for ((key, value) <- executorActor) value.send(SetExpressionExecutor(list, classname, rddID))
	}

	def setLatency(lat: Int): Unit = {
		latency = lat
	}

	def initRequestThread() {
		requestThread.start();
	}

	/**
	 * Request watchpoint data from all the partitions
	 */

	def requestData(): Unit = {
		for ((key, exec) <- executorActor) exec.send(RequestIntermediateData())
	}

	/**
	 * WatchPoint Handling at Drivers side
	 *
	 **/

	def enrollWatchpointRDD(id: Int, rdd: WatchPointLRDD[_, _]): Unit = {
		watchpointed_rdds(id) = rdd;
	}

	def extractWatchpointRDD(id: Int): WatchPointLRDD[_, _] = {
		watchpointed_rdds.getOrElse(id, null)
	}

	def getWatchpointIterator(tId: Int, rddid: Int) = {
		watchpointed_data.synchronized {
			watchpointed_data((tId, rddid)).iterator
		}
	}

	def getWatchpointIterator(rddid: Int) = {
		watchpointed_data.synchronized {
			var temp_list:
			List[CapturedRecord] = List()
			for (k <- watchpointed_data.keySet) {
				if (k._2 == rddid) {
					temp_list = temp_list ::: watchpointed_data(k)
				}
			}
			temp_list.takeRight(bigdebugConfiguration.K_WATCHPOINT_RECORDS).toIterator
		}
	}

	/**
	 * Used in BackendSchduler to recieve the captured data records from
	 * a watchpoint in a particular partition --Tag bigdebug @ Gulzar 06/20
	 **/
	def enrollWatchpointDataFromParition(list: List[CapturedRecord], rddID: Int): Unit = {
		watchpointed_data.synchronized {
			for (c <- list) {
				watchpointed_data((c.partitionID, rddID)) ::= c
			}
		}
		updateWatchpointUI(rddID);
	}

	def updateWatchpointUI(rdd: Int): Unit = {
		DebugHelper.log("INFO", "TaskExecutorManager", s"Report Watchpoint to UI")
		if (sparkUI != null) {
			sparkUI.driverWebSocket.s.updateWatchPointRDDs(rdd)
		}
	}


	def dumpWatchpoint(): Unit = {
		val file = new File("/tmp/" + sparkContext.getConf.getAppId + "wp.bd")
		val bw = new BufferedWriter(new FileWriter(file))
		for ((k, v) <- watchpointed_data) {
			bw.write(k + "  -->  " + v + "\n")
		}
		bw.close()
	}

	def getWPKeys() = {
		watchpointed_data.keys
	}

	/**
	 * Stragglers Handling Starts here
	 */

	/* Alternate Implementation: Notify Driver of start time and let it check time span. and alert it on task finish */


	/* We'll do it later ..
	private var taskMetricInfo = HashMap[(Long /*tID*/ , Int /*SID*/ , String /*Executor ID*/ ), BDDMetric]().withDefaultValue(null)
	var currentStage = -1;


	def getUiProfileData(): String = {
		var list = List[(Long, Long, String)]()
		val currentTime = System.currentTimeMillis()
		for ((t, s, e) <- taskMetricInfo.keySet) {
			val link = "http://" + executorUI(e)._1 + ":" + executorUI(e)._2
			var time = 0L;
			if (taskMetricInfo(t, s, e).finished) {
				time = taskMetricInfo(t, s, e).executionDonetime - taskMetricInfo(t, s, e).executionstarttime
			}
			else {
				time = (currentTime - taskMetricInfo(t, s, e).executionstarttime)

			}
			list ::=(t, time, link)
		}
		var content = list.map { a =>
			if (a == null) {
				""
			} else {
				val r = a._1
				val t = a._2
				val l = a._3
				s"""{"label": "$r" , "y" : $t  , "link" : "$l"}"""
			}
		}
		content = content.dropWhile(x => x.equals(""))
		var str = ""
		if (content.size > 0) {
			str = content.toList.reduce(_ + "," + _)
		}
		str = "[" + {
			if (str.endsWith(",")) str.substring(0, str.length - 1) else str
		} + "]"
		DebugHelper.log("INFO", "TaskExecutionManager", s" Executor UI data /n $str /n")
		sparkUI.driverWebSocket.s.updateDebuggerTaskProfiling(str)
		str
	}


	def SetTaskMetricInfo(sID: Int, tID: Long, execID: String, time: Long): Unit = {
		DebugHelper.log("INFO", "TaskExecutionManager", s" Executor Profiler  $sID, $tID, $execID, $time")

		if (currentStage != sID) {
			currentStage = sID
			// taskMetricInfo = taskMetricInfo.drop(taskMetricInfo.size)
		}
		val time1 = System.currentTimeMillis()
		val bddmetric = new BDDMetric
		bddmetric.executionstarttime = time1
		bddmetric.executionSpan = time1
		taskMetricInfo((tID, sID, execID)) = bddmetric
		if (!requestThreadstatus) {
			initRequestThread()
			requestThreadstatus = true;
		}
	}

	// Requester Disabled => SetTaskUpdateMetricInfo disabled
	def SetTaskUpdateMetricInfo(sID: Int, tID: Long, execID: String, time: Long): Unit = {
		DebugHelper.log("INFO", "TaskExecutionManager", s" Executor Profiler  $sID, $tID, $execID, $time")
		val time1 = System.currentTimeMillis()
		val bddmetric = taskMetricInfo.getOrElse((tID, sID, execID), null)
		if (bddmetric != null) {
			if (bddmetric.finished == false) {
				bddmetric.executionSpan = time1;
				taskMetricInfo((tID, sID, execID)) = bddmetric
			}
		}
	}

	def SetTaskDoneMetric(sID: Int, tID: Long, execID: String, time: Long): Unit = {
		DebugHelper.log("INFO", "TaskExecutionManager", s" Executor Done  $sID, $tID, $execID, $time")
		val time1 = System.currentTimeMillis()
		val bddmetric = taskMetricInfo.getOrElse((tID, sID, execID), null)
		if (bddmetric != null) {
			bddmetric.executionSpan = time1
			bddmetric.executionDonetime = time1
			bddmetric.finished = true
		}
		taskMetricInfo((tID, sID, execID)) = bddmetric
	}

	// Used in Requestor Thread :  Disabled.
	def requestExecutorsLocalTime(): Unit = {
		for (((tID, sID, execID), value) <- taskMetricInfo) executorActor(execID).send(RequestLocalTime(sID, tID))
	}

	def printStragglers(): Unit = {
		val time1 = System.currentTimeMillis()
		var sum = 0L
		var sumSQ = 0L
		var count = 0
		var span = 0L
		for (((tID, sID, execID), value) <- taskMetricInfo) {
			if (!value.finished) {
				span = (time1 - value.executionstarttime)
				sum = sum + span
				sumSQ = sumSQ + (span * span)
				count += 1
			} else {
				span = (value.executionDonetime - value.executionstarttime)s.record, s.stageID, s.taskID, s.rddID
				sum = sum + span
				sumSQ = sumSQ + (span * span)
				count += 1
			}
		}

		val avg = sum.asInstanceOf[Float] / count.asInstanceOf[Float]
		val std = Math.sqrt(((sumSQ.asInstanceOf[Float] / count.asInstanceOf[Float]) - (avg * avg)))
		DebugHelper.log("INFO", "TaskExecutionManager", "Finding Stragglers... (Current Avg: " + avg + " Current Stdev:" + std + " )")
		for (((tID, sID, execID), value) <- taskMetricInfo) {
			if (!value.finished) {
				span = (time1 - value.executionstarttime)
				if (span > ((2 * std) + avg)) {
					DebugHelper.log("INFO", "TaskExecutionManager", "Stragglers : (SubtaskID:" + sID + ", TaskID:" + tID + ", Worker:" + execID + ") Time Span => " + (span))
				}
			} else {
				span = (value.executionDonetime - value.executionstarttime)
				if (span > ((2 * std) + avg)) {
					DebugHelper.log("INFO", "TaskExecutionManager", "Stragglers :(SubtaskID:" + sID + ", TaskID:" + tID + ", Worker:" + execID + ") Finished in => " + span)
				}
			}
			DebugHelper.log("INFO", "TaskExecutionManager", "    (SubtaskID:" + sID + ", TaskID:" + tID + ", Worker:" + execID + ") => " + span)
		}
	}

*/


	/**
	 * Crash Culprit Handling at Driver
	 */

	def catchException(crashingRecord: CrashingRecord): Unit = {
		DebugHelper.log("INFO", "TaskExecutorManager", s"Catch exception( $crashingRecord")
	}

	private val crash_cuplrit_lock = HashMap[(Int, Int, Int), CrashingRecord]()
	var currentCrash: CrashingRecord = null
	var currentCrashSkip: Boolean = true
	var unresolvedCrashes = mutable.HashMap[(Int, Int, Int), List[CrashingRecord]]().withDefaultValue(Nil)
	// stage partition and rdd
	var resolvedCrashes = mutable.HashMap[(Int, Int, Int), List[CrashingRecord]]().withDefaultValue(Nil)
	var actualTaskDone = mutable.HashMap[(Int, Int, Int), Boolean]().withDefaultValue(false)


	def setActualTaskCompleted(stageID: Int, taskID: Int, subtaskID: Int, status: Boolean): Unit = {
		actualTaskDone((stageID, taskID, subtaskID)) = status
		//  println( s" Task Complete (  $stageID , $taskID , $subtaskID ) $status ")


		if (bigdebugConfiguration.MAP_ALL_CRASHES_ON_ONE == 1) {
			if (taskID + 1 == taskSetSize && status && resolvedCrashes((stageID, taskID, subtaskID)).length == unresolvedCrashes((stageID, taskID, subtaskID)).length) {
				val c_record = crash_cuplrit_lock.getOrElse((stageID, taskID, subtaskID), null)

				executorActor(c_record.senderId).send(ResolveAllCrashes(resolvedCrashes((stageID, taskID, subtaskID)), stageID, taskID, subtaskID))
				resolvedCrashes.remove((stageID, taskID, subtaskID))
				unresolvedCrashes.remove((stageID, taskID, subtaskID))
				crash_cuplrit_lock.remove((stageID, taskID, subtaskID))
			} else {

				if (taskID + 1 != taskSetSize) {
					val c_record = crash_cuplrit_lock.getOrElse((stageID, taskID, subtaskID), null)
					executorActor(c_record.senderId).send(ResolveAllCrashes(null, stageID, taskID, subtaskID))
					resolvedCrashes.remove((stageID, taskID, subtaskID))
					unresolvedCrashes.remove((stageID, taskID, subtaskID))
					crash_cuplrit_lock.remove((stageID, taskID, subtaskID))
				}
			}

		} else if (status && resolvedCrashes((stageID, taskID, subtaskID)).length == unresolvedCrashes((stageID, taskID, subtaskID)).length) {
			val c_record = crash_cuplrit_lock.getOrElse((stageID, taskID, subtaskID), null)
			executorActor(c_record.senderId).send(ResolveAllCrashes(resolvedCrashes((stageID, taskID, subtaskID)), stageID, taskID, subtaskID))
			resolvedCrashes.remove((stageID, taskID, subtaskID))
			unresolvedCrashes.remove((stageID, taskID, subtaskID))
			crash_cuplrit_lock.remove((stageID, taskID, subtaskID))
		} else {
			DebugHelper.log("INFO", "TaskExecutorManager", s"All crashes not resolved ( $stageID , $taskID , $subtaskID")
		}
	}


	def getCurrentCrashCulprit: CrashingRecord= {
		if (currentCrash == null) {
			CrashingRecord("", 0, 0, 0, new NullPointerException(), 0, "")
		}else
			currentCrash

	}

	/** *
	  *
	  * Get rdd Ids which have crashes
	  * */

	def getCrashIterator(id: Int): Iterator[CrashingRecord] = {
		var temp_list:
		List[CrashingRecord] = List()

		unresolvedCrashes.synchronized {

			for (k <- unresolvedCrashes.keySet) {
				if (k._3 == id) {
					temp_list = temp_list ::: unresolvedCrashes(k)
				}
			}
		}
		if (temp_list.size == 0 && currentCrash != null) {
			temp_list = List(currentCrash)
		}
		temp_list.toIterator
	}

	def crashedRDDList(): Set[Int] = {
		var crashed_rdds = Set[Int]()
		for (k <- unresolvedCrashes.keySet) {
			if (unresolvedCrashes(k).size > 0) {
				crashed_rdds += k._3
			}
		}
		if (crashed_rdds.size == 0 && currentCrash != null) {
			crashed_rdds = Set(currentCrash.rddid)
		}
		crashed_rdds
	}

	def parseLineNumber(str: String): String = {
		if (str.contains("scala:")) {
			val start = str.indexOf("scala:")
			val v = str.substring(start + 6, str.length)
			return v.trim()
		} else {
			return "0"
		}
	}

	def updateCrashUI(rdd: Int): Unit = {
		DebugHelper.log("INFO", "TaskExecutorManager", s"Report to UI")
		if (sparkUI != null) {
			sparkUI.driverWebSocket.s.sendToAll(DebuggerPageUtils.getDAGMetaData(sparkUI.operationGraphListener.getOperationGraphForJob(0)).toString())
			sparkUI.driverWebSocket.s.updateCrashedRDDs(rdd)
			sparkUI.driverWebSocket.s.sendToAll("Error " + parseLineNumber(getRDDDetails(rdd)))
		}
	}

	def getLineageofCrashingRecord(rddID: Int, lineageID: (Int, Any)): Long = {
		if (lineageContext == null) {
			return -1L
		}

		val rdd = getRDDFromId(rddID)

		def getTapRDD(rdd: RDD[_]): TapLRDD[_] = {
			rdd.dependencies(0).rdd match {
				case tap: TapLRDD[_] => tap
				case other => getTapRDD(other)
			}
		}

		//val tap = getTapRDD(rdd)
		val tap = if (rdd.isInstanceOf[TapLRDD[_]]) rdd else getTapRDD(rdd)

		disableTopRDDset = true;
		tap match {
			case h: TapHadoopLRDD[_, _] =>
				getLineage[Long, String](tap.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
					.map(r => (r._1.get(), r._2.toString)), lineageID._2.asInstanceOf[Long])
					.cache().collect().foreach(println)
			case t: TapLRDD[_] =>
				//				lineageContext.setCulprit()
				//				lineageContext.setCurrentLineagePosition(Some(t))
				//				lineageContext.getLineage(t)
				//				val tmp = lineageContext.getBackward(0).get.map(r => (r._1._2, r._2.asInstanceOf[Array[Int]]))
				//				val lineage = new LineageRDD(getLineage[Int, Array[Int]](tmp, lineageID._2.asInstanceOf[Int]).flatMap(r => r._2.map(b => (r._1, (Dummy, b)))))
				//				lineage.goBackAll().show
				lineageContext.setCulprit()
				// set last Lineage
				lineageContext.setCurrentLineagePosition(Some(t))
				lineageContext.getLineage(t)
				val tmp = tap.asInstanceOf[Lineage[(Any, Array[Int])]]
				//               get.map(r => (r._1._2, r._2.asInstanceOf[Array[Int]]))
				val lineage = new LineageRDD(getLineage(tmp, lineageID).flatMap(r => r._2.map(b => (r._1, (Dummy, b)))))
				//flatMap(r => r._2.map(b => (r._1.asInstanceOf[Tuple2[Any, Any]]._2, b))))
				val a = lineage.goBackAll()
				// a.collect().foreach(println)
				// println("showing lineage now")
				a.show().collect().foreach(println)
			//sparkContext.cancelJob(currenJobID)
		}
		disableTopRDDset = false;
		0L
	}

	def getLineage[T, V](prev: Lineage[(T, V)], next: T) = {
		prev.filter {
			current =>
				current._1 == next
		}
	}

	def startLineageQuery(h_code: Int, stage: Int, task: Int, subtask: Int): Unit = {
		// println(s"Starting  Lineage")
		var lin_id: (Int, Any) = (0, null)
		var rddId = -1
		if (currentCrash.lineageID.hashCode() == h_code) {
			lin_id = currentCrash.lineageID
			rddId = currentCrash.rddid
		} else {
			for (record <- unresolvedCrashes(stage, task, subtask)) {
				if (record.lineageID.hashCode() == h_code) {
					lin_id = record.lineageID
					rddId = record.rddid
				}
			}
		}
		getLineageofCrashingRecord(rddId, lin_id)
	}

	def enrollCrash(c: CrashingRecord): Unit = {
		DebugHelper.log("INFO", "TaskExecutorManager", s"Enroll Crash ( $c")
		recordStore((c.stageID, c.taskID, c.rddid)) ::= c
		crash_cuplrit_lock((c.stageID, c.taskID, c.rddid)) = c
		currentCrash = c
		if (!c.blocking) {
			if (bigdebugConfiguration.MAP_ALL_CRASHES_ON_ONE == 1) {
				unresolvedCrashes((c.stageID, taskSetSize - 1, c.rddid)) ::= c // Map onto the last task
			} else {
				unresolvedCrashes((c.stageID, c.taskID, c.rddid)) ::= c
			}
		}
		updateCrashUI(c.rddid)
	}

	/**
	 * Retrieve Crashing record from the unresolved list
	 * */
	def fixUnresolvedCrashingRecord(crash: String, stage:Int, task:Int, rdd :Int , srnum : Int , action: Int): Unit ={
		val list = unresolvedCrashes((stage, task, rdd)).filter( s => s.srnumn == srnum )
		if(!list.isEmpty){
			takeActionCrashCuplrit(list.head , action)
		}else{
			DebugHelper.log("INFO", "TaskExecutorManager", s"No corresponding unresolved crashing record found: ( $crash")
		}
	}
	def takeActionCrashCuplrit(c: CrashingRecord, action: Int /*0 for skip , 1 for modify*/): Unit = {
		val c_record = crash_cuplrit_lock.getOrElse((c.stageID, c.taskID, c.rddid), null)
		DebugHelper.log("INFO", "TaskExecutorManager", s"Action  Crash ( $c)")
		if (c_record.blocking) {
			crash_cuplrit_lock.remove((c.stageID, c.taskID, c.rddid))
			executorActor(c_record.senderId).send(CrashRecordAction(action /*1 for modify 0 for skip*/ , c))
		} else {
			if (action == 1) {
				if (bigdebugConfiguration.MAP_ALL_CRASHES_ON_ONE == 1) {
					resolvedCrashes((c.stageID, taskSetSize - 1, c.rddid)) ::= c
				} else {
					resolvedCrashes((c.stageID, c.taskID, c.rddid)) ::= c

				}
			}
			else {
				if (bigdebugConfiguration.MAP_ALL_CRASHES_ON_ONE == 1) {
					resolvedCrashes((c.stageID, taskSetSize - 1, c.rddid)) ::= null
				} else {
					resolvedCrashes((c.stageID, c.taskID, c.rddid)) ::= null

				}
			}

			if (bigdebugConfiguration.MAP_ALL_CRASHES_ON_ONE == 1) {

				if (resolvedCrashes((c.stageID, taskSetSize - 1, c.rddid)).length == unresolvedCrashes((c.stageID, taskSetSize - 1, c.rddid)).length &&
					isLastTaskDone(c.stageID, c.rddid)) {
					val c_record = crash_cuplrit_lock.getOrElse((c.stageID, taskSetSize - 1, c.rddid), null)
					executorActor(c_record.senderId).send(ResolveAllCrashes(resolvedCrashes((c.stageID, taskSetSize - 1, c.rddid)), c.stageID, taskSetSize - 1, c.rddid))
					resolvedCrashes.remove((c.stageID, taskSetSize - 1, c.rddid))
					unresolvedCrashes.remove((c.stageID, taskSetSize - 1, c.rddid))
					crash_cuplrit_lock.remove((c.stageID, taskSetSize - 1, c.rddid))

				}


			} else if (resolvedCrashes((c.stageID, c.taskID, c.rddid)).length == unresolvedCrashes((c.stageID, c.taskID, c.rddid)).length && actualTaskDone((c.stageID, c.taskID, c.rddid))) {
				// println("All crashes resolved ( " + stageID + "," + taskID + "," + subtaskID + ") ")
				DebugHelper.log("INFO", "TaskExecutorManager", s"All crashes resolved ( $c ) ")
				executorActor(c_record.senderId).send(ResolveAllCrashes(resolvedCrashes((c.stageID, c.taskID, c.rddid)), c.stageID, c.taskID, c.rddid))
				resolvedCrashes.remove((c.stageID, c.taskID, c.rddid))
				unresolvedCrashes.remove((c.stageID, c.taskID, c.rddid))
				crash_cuplrit_lock.remove((c.stageID, c.taskID, c.rddid))
			} else {
				DebugHelper.log("INFO", "TaskExecutorManager", s"All crashes not resolved ( $c )  ")

			}
		}

	}


	def isLastTaskDone(stage: Int, subtask: Int): Boolean = {
		actualTaskDone.getOrElse((stage, taskSetSize - 1, subtask), false)
	}

	private var taskSetSize: Int = -1

	def setTaskSetSize(tasks: Int) = {
		taskSetSize = tasks
	}

	/**
	 *
	 * Breakpoint
	 *
	 */

	private var breakpointStageMap = HashMap[Int, Int]()
//
//	def assignStagesToBreakpoints(finalStage: Stage): Unit = {
//		var currentStage = finalStage.id
//		var parent = finalStage.parents
//		var upper = finalStage.rdd.id
//		while (parent.size > 0) {
//			var lower = parent(0).rdd.id
//			for (a <- lower + 1 to upper) {
//				breakpointStageMap(a) = currentStage
//			}
//			currentStage = parent(0).id
//			parent = parent(0).parents
//			upper = lower
//		}
//		for (a <- 0 to upper) {
//			breakpointStageMap(a) = currentStage
//		}
//	}

	var sparkContext: SparkContext = null
	var lineageContext: LineageContext = null
	var patched: Boolean = false
	var bp: RDD[_] = null
	var currentbreakpointLocation: Int = -1
	var bpThreadPool: List[Thread] = List[Thread]()
	var currentJobID: Int = -1
	var bpRDD: mutable.Queue[RDD[_]] = Queue[RDD[_]]()

	def setSparkContext(sc: SparkContext): Unit = {
		sparkContext = sc
	}

	def setLineageContext(lc: LineageContext): Unit = {
		lineageContext = lc
	}


	def enrollBreakpoint[T](b: RDD[T]): Unit = {
		if (bp == null) {
			bp = b
			currentbreakpointLocation = b.id
		} else {
			bpRDD.enqueue(b)
		}
	}

	def stepOver() {
		if (bpRDD.size > 0) {
			val tempbp = bpRDD.front
			if (tempbp.id <= currentbreakpointLocation + 1) {
				bpRDD.dequeue()
			}
		}
		startNewjob()
		currentbreakpointLocation = currentbreakpointLocation + 1
	}

	def getCurrentBreakpointRDDDetails(): String = {
		val rdd = getRDDFromId(currentbreakpointLocation)
		if (rdd != null) {
			return rdd.getCreationSite
		}
		""
	}

	def getRDDDetails(id: Int): String = {
		val rdd = getRDDFromId(id)
		if (rdd != null) {
			return rdd.getCreationSite
		}
		""
	}

	def resetBreakpoint[T](i: Int): Unit = {
		currentbreakpointLocation = i
	}

	var breakpointThread = new Thread() {
		override def run() {
			this.synchronized {
				topRDD.collect().foreach(println)
				this.notifyAll()
			}
		}
	}

	def getNewThread(): Thread = {
		new Thread() {
			override def run() {
				this.synchronized {
					topRDD.collect().foreach(println)
					this.notifyAll()
					notify_All()
				}
			}
		}
	}

	def resume(): Unit = {
		if (bp != null) {
			if (patched) {
				startNewjob()
			} else {
				if (bpRDD.size != 0) {
					bp = bpRDD.dequeue()
					currentbreakpointLocation = bp.id
					return
				} else notify_All()
			}
		}
		currentbreakpointLocation = topRDD.id
		bp = null
	}

	def kill(): Unit = {
		sparkContext.cancelJob(currenJobID + 1)
	}


	def notify_All(): Unit = {
		breakpointThread.synchronized {
			breakpointThread.notifyAll()
		}
		// System.exit(1)
		for (a <- bpThreadPool) {
			a.synchronized {
				a.notifyAll()
			}
		}
	}

	def startNewjob() {
		if (currentbreakpointLocation == topRDD.id) {
			notify_All()
		} else {
			kill()
			bpThreadPool = breakpointThread :: bpThreadPool
			breakpointThread = getNewThread()
			breakpointThread.synchronized {
				breakpointThread.start
			}
		}
	}

	def getBreakPointWaitingThread: Unit = {
		if (bp != null) {
			breakpointThread.synchronized {
				breakpointThread.wait()
			}
		}
	}


	/**
	 * Backword and Farwords Tracing starts here
	 */
	private val recordStore = mutable.HashMap[(Int /*Stage ID*/ , Int /*TaskID*/ , Int /*SubtaskID*/ ), List[CrashingRecord]]().withDefaultValue(Nil)

	def getCrashingLineageId: Map[(Int /*Stage ID*/ , Int /*TaskID*/ , Int /*SubtaskID*/ ), List[CrashingRecord]] = {
		return recordStore
	}

	/** *Runtime Code fix */

	private var codeFixToRDDid = HashMap[Int, String]()

	def resetCode(rddid: Int, code: String): Unit = {
		//  println(" Code Fix")
		val rdd = getRDDFromId(rddid)
		val name = Lineage.findRDDTypeAndFix(rdd, code)
		codeFixToRDDid(rddid) = name
		patched = true
	}

	def deepCopy[A](f: A): A = {
		var i: ObjectInputStream = null
		var o: ObjectOutputStream = null
		try {
			val bos: ByteArrayOutputStream = new ByteArrayOutputStream()
			o = new ObjectOutputStream(bos)
			o.writeObject(f)
			o.flush()
			val bin: ByteArrayInputStream =
				new ByteArrayInputStream(bos.toByteArray())
			i = new ObjectInputStream(bin)
			val f_new = i.readObject()
			o.close()
			i.close()
			return f_new.asInstanceOf[A]

		} catch {
			case e: Exception =>
				o.close()
				i.close()
				e.printStackTrace()
		}
		f
	}

	def retrieveCodeFix(): HashMap[Int, (String, List[Int])] = {
		var h = HashMap[Int, (String, List[Int])]()
		for (k <- codeFixToRDDid.keySet) {
			val name = codeFixToRDDid(k)
			//    println("***" + name + " , " + k + "***")
			h(k) = (name, readPredicateClass(name))

		}
		h
	}

	var crashFound = false;

	def foundCrash(set: Boolean): Unit = {
		crashFound = set
	}

	def isCrashFound() = crashFound


	/** 05/12 **/

	def batchModifyCrashRecords(rddid: Int, code: String) = {
		//  println(s" Batch modified recieved at rdd: $rddid")
		val rdd = getRDDFromId(rddid)
		val crashedRecords = getCrashIterator(rddid)
		//  val size  = crashedRecords.length
		// println(s" Total $size crashed to be modified")
		val fixedRecordsList = Lineage.findCrashedRDDTypeAndFix(rdd, code, crashedRecords)
		// println(s" Modifier compiled and crashes fixed")
		while (fixedRecordsList.hasNext) {
			val s = fixedRecordsList.next()
			takeActionCrashCuplrit(s, 1)
		}
	}


}


/** 05/12 **/


/** BDD END **/