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

package org.apache.spark.ui.debugger

import javax.servlet.http.HttpServletRequest

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.bdd._
import org.apache.spark.scheduler._
import org.apache.spark.ui._
import org.apache.spark.{Partition, SparkContext}

import scala.collection.immutable.HashMap
import scala.xml.Node

private[ui] class DebuggerTab(parent: SparkUI) extends SparkUITab(parent, "debugger") {
  val objectManager = new DebuggerObjectManager()
  //val listener = new DebuggerListener(objectManager, parent.sc.get)

  /** For DAG Viz */
  val operationGraphListener = parent.operationGraphListener

  /** Ends here */
  attachPage(new DebuggerPage(this, objectManager))
  attachPage(new WatchpointPage(this))

  attachPage(new RDDPage(this))
  attachPage(new RDDStPage(this))

  //  parent.registerListener(listener)
  // ***************** Listener not available in 1.2.1 Fix it ***************
  def getSparkContext: SparkContext = {
    parent.sc.get
  }


  def getDriverWebSocketPort: String = {
    parent.driverWebSocket.getCurrentPort().toString
  }

  def handleDebuggerCommand(request: HttpServletRequest): Unit = {
    val command: String = Option(request.getParameter("command")).getOrElse("")
    command match {
      case "terminate" =>
        parent.shouldBeTerminated = true
      case "killandrun" =>
        println(" In Kill and RUn")
        TaskExecutionManager.startNewjob()
      case "resume" =>
        TaskExecutionManager.resume()
      case "stepover" =>
        TaskExecutionManager.stepOver()
      case "setnewrddfunc" =>
        val code = Option(request.getParameter("code")).getOrElse("")
        val rddid = Option(request.getParameter("rddid")).getOrElse("").toInt
        TaskExecutionManager.resetCode(rddid,code )
      case "batchmodifyfunction" => /***05/12*/
        val code = Option(request.getParameter("code")).getOrElse("")
        val rddid = Option(request.getParameter("rddid")).getOrElse("").toInt
        ///println(s" Batch Code \n\n $code \n\n\n  $rddid")
        TaskExecutionManager.batchModifyCrashRecords(rddid,code)
      case "dump_intratask" =>
        TaskExecutionManager.dumpWatchpointData()
      case "resolve" =>
        val crash: String = Option(request.getParameter("record")).getOrElse("")
        val stage: Int = Option(request.getParameter("stage")).getOrElse("").toInt
        val task: Int = Option(request.getParameter("task")).getOrElse("").toInt
        val subtask: Int = Option(request.getParameter("subtask")).getOrElse("").toInt
        //   TaskExecutionManager.simulateCrashResolution(stage, task , subtask)
        println(s"Resolve Crash $crash \n $stage" +
          s" $task $subtask")
        TaskExecutionManager.takeActionCrashCuplrit(crash, stage, task, subtask, 1)
      case "trace" =>
        val stage: Int = Option(request.getParameter("stage")).getOrElse("").toInt
        val task: Int = Option(request.getParameter("task")).getOrElse("").toInt
        val subtask: Int = Option(request.getParameter("subtask")).getOrElse("").toInt
        val h_code: Int = Option(request.getParameter("linid")).getOrElse("").toInt
        println("Invoking Lineage Query")
        TaskExecutionManager.startLineageQuery(h_code , stage, task, subtask)
      case "skip" =>
       // val crash: String = Option(request.getParameter("record")).getOrElse("")
        val stage: Int = Option(request.getParameter("stage")).getOrElse("").toInt
        val task: Int = Option(request.getParameter("task")).getOrElse("").toInt
        val subtask: Int = Option(request.getParameter("subtask")).getOrElse("").toInt
        //   TaskExecutionManager.simulateCrashResolution(stage, task , subtask)
        println("Skipping Record")
        TaskExecutionManager.takeActionCrashCuplrit("", stage, task, subtask, 0)
      case "test" =>
      // NOTE: If we need to run something simple, we can use this command.
      case "set_guard" => {
        val regex = Option(request.getParameter("code")).getOrElse("")
        val rddid = Option(request.getParameter("rddid")).getOrElse("").toInt
        TaskExecutionManager.setExpression(regex, rddid)
      }
      case _ => {
        // do nothing
        DebugHelper.log("ERROR", "handleDebuggerCommand", "Invalid Command")
      }
    }
  }
}

class DebuggerStageObject(val stageId: Int) {
  def setRDDDetailedInfo(rddDetailedInfo: String) = {
    this.rddDetailedInfo = rddDetailedInfo
  }

  private var rddDetailedInfo: String = "undefined"

  def getRDDDetailedInfo = rddDetailedInfo

  def setName(stageName: String) = {
    this.stageName = stageName
  }

  private var stageName: String = "undefined"

  def getName: String = stageName

  def setRDDInfo(rddName: String) = {
    this.rddInfo = rddName
  }

  def getRDDInfo: String = rddInfo

  private var rddInfo: String = "undefined"
  private var currentStatus: String = "undefined"
  private var childTasks = List[DebuggerTaskObject]()

  def setCurrentStatus(newStatus: String) = {
    this.currentStatus = newStatus
  }

  def getCurrentStatus: String = currentStatus

  def addOrGetChildTask(index: Long): DebuggerTaskObject = {
    val obj = getChildTask(index)
    if (obj == null) {
      val newObj = new DebuggerTaskObject(index, this)
      childTasks = childTasks ::: List(newObj)
      newObj
    }
    else {
      obj
    }
  }

  def getChildTasks: List[DebuggerTaskObject] = childTasks

  def getChildTask(index: Long): DebuggerTaskObject = {
    childTasks.find((e => e.index == index)).getOrElse(null)
  }
}

class DebuggerTaskObject(var index: Long, val parent: DebuggerStageObject) {
  private var detailedInfo: String = ""

  def setDetailedInfo(detailedInfo: String): Unit = {
    this.detailedInfo = detailedInfo
  }

  def getDetailedInfo: String = detailedInfo

  def setRDDName(rddName: String) = {
    this.rddName = rddName
  }

  def getRDDName: String = rddName

  private var rddName: String = "undefined"

  private var taskType: String = "undefined"

  def setTaskType(newTaskType: String) = {
    this.taskType = newTaskType
  }

  /*
  * possible task type: ShuffleMapTask, ResultTask
  * */

  def getTaskType: String = taskType

  private var currentStatus: String = "undefined"

  def setCurrentStatus(newStatus: String) = {
    this.currentStatus = newStatus
  }

  def getCurrentStatus: String = currentStatus

  private var subtaskInfo = new HashMap[Int, String]()

  def setSubtaskInfo(subtaskIndex: Int, description: String) = {
    subtaskInfo += (subtaskIndex -> description)
  }

  def getSubtaskInfo(): HashMap[Int, String] = this.subtaskInfo

  private var executorId: String = ""

  def setExecutorId(executorId: String) = {
    this.executorId = executorId
  }

  def getExecutorId(): String = this.executorId
}

class DebuggerObjectManager {
  def renderContentForStages(objectToNodes: (DebuggerStageObject) => Seq[Node]) = {
    stageObjects.map(objectToNodes)
  }

  var stageObjects = List[DebuggerStageObject]()

  def getStageObject(stageId: Int): DebuggerStageObject = {
    synchronized {
      stageObjects.find((e => e.stageId == stageId)).getOrElse(null)
    }
  }

  def addOrGetStageObject(stageId: Int): DebuggerStageObject = {
    synchronized {
      val obj = getStageObject(stageId)
      if (obj == null) {
        val newObj = new DebuggerStageObject(stageId)
        stageObjects = stageObjects ::: List(newObj)
        newObj
      }
      else {
        obj
      }
    }
  }
}
