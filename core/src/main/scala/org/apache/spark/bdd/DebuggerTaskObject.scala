package org.apache.spark.bdd

import scala.collection.mutable.HashMap

class DebuggerTaskObject(var taskIndex: Int) {
  private var taskType: String = "undefined"
  private var taskStatus: String = "undefined"
  private var rddName: String = "undefined"
  private var detailedInfo: String = ""
  private var subtaskInfo = new HashMap[Int, String]()
  private var executorId: String = ""

  def getTaskType: String = taskType

  def setTaskType(newTaskType: String) = {
    this.taskType = newTaskType
  }

  def getCurrentStatus: String = taskStatus

  def setCurrentStatus(newStatus: String) = {
    this.taskStatus = newStatus
  }

  def getRDDName: String = rddName

  def setRDDName(rddName: String) = {
    this.rddName = rddName
  }

  def getDetailedInfo: String = detailedInfo

  def setDetailedInfo(detailedInfo: String): Unit = {
    this.detailedInfo = detailedInfo
  }

  def getSubtaskInfo(): HashMap[Int, String] = this.subtaskInfo

  def setSubtaskInfo(subtaskIndex: Int, description: String) = {
    subtaskInfo += (subtaskIndex -> description)
  }

  def getExecutorId(): String = this.executorId

  def setExecutorId(executorId: String) = {
    this.executorId = executorId
  }
}