package org.apache.spark.bdd

import scala.collection.mutable.HashMap

class DebuggerStageObject(val stageId: Int) {
  private var stageName: String = "undefined"
  private var currentStatus: String = "undefined"
  private var rddInfo: String = "undefined"
  private var rddDetailedInfo: String = "undefined"
  private var childTasks = new HashMap[Int, DebuggerTaskObject]()

  def setRDDDetailedInfo(rddDetailedInfo: String) = {
    this.rddDetailedInfo = rddDetailedInfo
  }

  def getRDDDetailedInfo = rddDetailedInfo

  def setName(stageName: String) = {
    this.stageName = stageName
  }

  def getName: String = stageName

  def setRDDInfo(rddName: String) = {
    this.rddInfo = rddName
  }

  def getRDDInfo: String = rddInfo

  def setCurrentStatus(newStatus: String) = {
    this.currentStatus = newStatus
  }

  def getCurrentStatus: String = currentStatus

  def addOrGetChildTask(taskIndex: Int): DebuggerTaskObject = {
    childTasks.synchronized {
      val obj = getChildTask(taskIndex)
      if (obj == null) {
        val newObj = new DebuggerTaskObject(taskIndex)
        childTasks(taskIndex) = newObj
        newObj
      }
      else {
        obj
      }
    }
  }

  def getChildTasks = childTasks

  def getChildTask(taskIndex: Int): DebuggerTaskObject = {
    var ret: DebuggerTaskObject = null
    childTasks.synchronized {
      ret = childTasks.get(taskIndex).getOrElse(null).asInstanceOf[DebuggerTaskObject]
    }
    ret
  }
}
