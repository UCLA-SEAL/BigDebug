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

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._
import org.apache.spark.ui._

import org.apache.spark.bdd._

import scala.collection.immutable.{Nil, HashMap}
import scala.xml.Node

private[ui] class DebuggerTab(parent: SparkUI) extends SparkUITab(parent, "debugger") {
  val objectManager = new DebuggerObjectManager()
  val listener = new DebuggerListener(objectManager, parent.sc)

  attachPage(new DebuggerPage(this, objectManager))
  parent.registerListener(listener)

  def getSparkContext:SparkContext = {
    parent.sc
  }

  def handleDebuggerCommand(request: HttpServletRequest) = {
    val command: String = Option(request.getParameter("command")).getOrElse("")
    command match {
      case "terminate" => {
        parent.shouldBeTerminated = true
      }
      case "pause_all" => {
      }
      case "resume_all" => {
        TaskExecutionManager.ResumeAll()
      }
      case "resume_task" => {
        val stageId: Int = Option(request.getParameter("stage_id")).getOrElse("-1").toInt
        val index: Long = Option(request.getParameter("index")).getOrElse("-1").toLong
        if (stageId != -1 && index != -1 ){
          TaskExecutionManager.Resume(stageId,index)
        }
        else{
          println("** wrong parameters **")
        }
      }
      case "toggle_breakpoint_before" => {
        val stageId: Int = Option(request.getParameter("stage_id")).getOrElse("-1").toInt
        val index: Long = Option(request.getParameter("index")).getOrElse("-1").toLong
        if (stageId != -1 && index != -1 ){
          TaskExecutionManager.ToggleBreakpointBeforeRunning(stageId,index)
        }
        else{
          println("** wrong parameters **")
        }
      }
      case "toggle_breakpoint_after" => {
        val stageId: Int = Option(request.getParameter("stage_id")).getOrElse("-1").toInt
        val index: Long = Option(request.getParameter("index")).getOrElse("-1").toLong
        if (stageId != -1 && index != -1 ){
          TaskExecutionManager.ToggleBreakpointAfterRunning(stageId,index)
        }
        else{
          println("** wrong parameters **")
        }
      }
      case _ => {
        // do nothing
        println("** handleDebuggerCommand ** unsupported command: " + command)
      }
    }
  }
}

class DebuggerStageObject(val stageId: Int) {
  def setRDDDetailedInfo(rddDetailedInfo: String) = {
    this.rddDetailedInfo=rddDetailedInfo
  }
  private var rddDetailedInfo:String="undefined"
  def getRDDDetailedInfo = rddDetailedInfo

  def setName(stageName: String) = {
    this.stageName=stageName
  }
  private var stageName:String="undefined"
  def getName:String = stageName

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

  def addOrGetChildTask(index:Long): DebuggerTaskObject = {
    val obj = getChildTask(index)
    if(obj==null){
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
  private var detailedInfo:String=""
  def setDetailedInfo(detailedInfo:String): Unit ={
    this.detailedInfo=detailedInfo
  }
  def getDetailedInfo:String=detailedInfo

  def setRDDName(rddName: String) = {
    this.rddName = rddName
  }
  def getRDDName:String = rddName
  private var rddName:String = "undefined"

  private var taskType: String = "undefined"
  def setTaskType(newTaskType:String)={
    this.taskType = newTaskType
  }
  def getTaskType:String = taskType

  private var currentStatus: String = "undefined"
  def setCurrentStatus(newStatus: String) = {
    this.currentStatus = newStatus
  }
  def getCurrentStatus: String = currentStatus
}

class DebuggerObjectManager{
  def renderContentForStages(objectToNodes: (DebuggerStageObject) => Seq[Node]) = {
    stageObjects.map(objectToNodes)
  }

  var stageObjects = List[DebuggerStageObject]()

  def getStageObject(stageId:Int): DebuggerStageObject ={
    synchronized {
      stageObjects.find((e=>e.stageId==stageId)).getOrElse(null)
    }
  }

  def addOrGetStageObject(stageId:Int): DebuggerStageObject ={
    synchronized {
      val obj = getStageObject(stageId)
      if(obj ==null){
        val newObj = new DebuggerStageObject(stageId)
        stageObjects = stageObjects ::: List(newObj)
        newObj
      }
      else{
        obj
      }
    }
  }
}

/**
 * :: DeveloperApi ::
 * A SparkListener that prepares information to be displayed on the DebuggerTab
 */
@DeveloperApi
class DebuggerListener(val objectManager:DebuggerObjectManager, val sc:SparkContext) extends SparkListener {

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println("** debugger listener ** stageCompleted")
    println(org.apache.spark.util.JsonProtocol.stageCompletedToJson(stageCompleted).toString)

    val stageId = stageCompleted.stageInfo.stageId
    val stageObject = objectManager.getStageObject(stageId)
    if (stageObject != null) {
      stageObject.setCurrentStatus("completed")
      stageObject.setName(stageCompleted.stageInfo.name)

      val stage = sc.dagScheduler.getStage(stageId)
      stageObject.setRDDInfo(stage.rdd.toString)
      stageObject.setRDDDetailedInfo(stage.rdd.toDebugString)
    }
    else {
      println("** debugger listener ** stage[" + stageId + "] is not found.")
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    println("** debugger listener ** stageSubmitted")
    println(org.apache.spark.util.JsonProtocol.stageSubmittedToJson(stageSubmitted).toString)

    val stageId = stageSubmitted.stageInfo.stageId
    val newObj = objectManager.addOrGetStageObject(stageId)
    newObj.setCurrentStatus("submitted")
    newObj.setName(stageSubmitted.stageInfo.name)

    val stage = sc.dagScheduler.getStage(stageId)
    newObj.setRDDInfo(stage.rdd.toString)
    newObj.setRDDDetailedInfo(stage.rdd.toDebugString)
  }

  override def onTaskSubmitted(taskSubmitted: SparkListenerTaskSubmitted): Unit = {
    println("** debugger listener ** taskSubmitted")
    println(org.apache.spark.util.JsonProtocol.taskSubmittedToJson(taskSubmitted).toString)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    println("** debugger listener ** taskStart")
    println(org.apache.spark.util.JsonProtocol.taskStartToJson(taskStart).toString)

    val stageId = taskStart.stageId
    val index = taskStart.taskInfo.index
    val stageObject = objectManager.getStageObject(stageId)
    if (stageObject != null) {
      val childTask = stageObject.addOrGetChildTask(index)
      childTask.setCurrentStatus("submitted")
      childTask.setTaskType(taskStart.taskInfo.taskType)
      childTask.setDetailedInfo(taskStart.taskInfo.taskDetailInfo)
    }
    else {
      println("** debugger listener ** stage[" + stageId + "] is not found.")
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    println("** debugger listener ** taskEnd")
    println(org.apache.spark.util.JsonProtocol.taskEndToJson(taskEnd).toString)

    val stageId = taskEnd.stageId
    val index = taskEnd.taskInfo.index
    val stageObject = objectManager.getStageObject(stageId)
    if (stageObject != null) {
      val childTask = stageObject.addOrGetChildTask(index)
      childTask.setCurrentStatus("completed")
      childTask.setTaskType(taskEnd.taskInfo.taskType)
      childTask.setDetailedInfo(taskEnd.taskInfo.taskDetailInfo)
    }
    else {
      println("** debugger listener ** stage[" + stageId + "] is not found.")
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    println("** debugger listener ** jobStart")
    println(org.apache.spark.util.JsonProtocol.jobStartToJson(jobStart).toString)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    println("** debugger listener ** jobEnd")
    println(org.apache.spark.util.JsonProtocol.jobEndToJson(jobEnd).toString)
  }
}
