/**
 *
 * Newer Version of BugDebug do not use this class
 *
 **/

package org.apache.spark.bdd

import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.HashMap;

class DebuggerInfoManager {
  private var stageObjects = new HashMap[Int, DebuggerStageObject]()
  private var lastLogicalClock: Integer = 0

  def getStatus: String = {
    synchronized {
      val json = ("stages" -> stageObjects.keys.map {
        stageId => {
          val stageObject = stageObjects(stageId)
          val status = stageObject.getCurrentStatus
          val name = stageObject.getName
          val rddDetailedInfo = stageObject.getRDDDetailedInfo
          val rddInfo = stageObject.getRDDInfo
          val childTasks = stageObject.getChildTasks
          val childTasksJson = ("tasks" -> childTasks.keys.map {
            taskId => {
              val childTask = childTasks(taskId)
              val status = childTask.getCurrentStatus
              val detailedInfo = childTask.getDetailedInfo
              val executorId = childTask.getExecutorId
              val rddName = childTask.getRDDName
              val taskType = childTask.getTaskType
              val subTaskInfo = childTask.getSubtaskInfo
              val subTaskInfoJson = ("subTaskInfo" -> subTaskInfo.keys.map { subTaskId => (("id" -> subTaskId.toString) ~ ("info" -> subTaskInfo(subTaskId)))})
              (("id" -> taskId.toString) ~ ("status" -> status) ~ ("detailedInfo" -> detailedInfo) ~ ("executorId" -> executorId) ~ ("rddName" -> rddName) ~ ("taskType" -> taskType) ~ subTaskInfoJson)
            }
          })
          (("id" -> stageId.toString) ~ ("status" -> status) ~ ("name" -> name) ~ ("rddDetailedInfo" -> rddDetailedInfo) ~ ("rddInfo" -> rddInfo) ~ childTasksJson)
        }
      })
      compact(render(json))
    }
  }

  def getLastLogicalClock: Integer = {
    synchronized {
      lastLogicalClock
    }
  }

  def updateLastLogicalClock(newLogicalClock: Integer) = {
    synchronized {
      if (this.lastLogicalClock < newLogicalClock) {
        this.lastLogicalClock = newLogicalClock
      }
    }
  }

  /**
   * @note The return variable 'DebuggerStageObject' is not thread-safe.
   * @param stageId
   * @return
   */
  def getStageObject(stageId: Int): DebuggerStageObject = {
    var ret: DebuggerStageObject = null
    synchronized {
      ret = stageObjects.get(stageId).getOrElse(null).asInstanceOf[DebuggerStageObject]
    }
    ret
  }

  /**
   * @note The return variable 'DebuggerStageObject' is not thread-safe.
   * @param stageId
   * @return
   */
  def addOrGetStageObject(stageId: Int): DebuggerStageObject = {
    synchronized {
      val obj = getStageObject(stageId)
      if (obj == null) {
        val newObj = new DebuggerStageObject(stageId)
        stageObjects(stageId) = newObj
        newObj
      }
      else {
        obj
      }
    }
  }
}