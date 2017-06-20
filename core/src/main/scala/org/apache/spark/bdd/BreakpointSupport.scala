/** BDD START
  *
  *
  * Not Used in the newer version of Big Debug
  *
  */

package org.apache.spark.bdd

import scala.collection.mutable._

object BreakpointManager {
  def getStatus: String = {
    "{}"
  }

  private val lockBreakpointManager = new Object()
  private val waitObjects = HashMap[(Int, Int, Long, Int), Object]()
  private var breakpointInfos = HashMap[(Int, Int, Long, Int), BreakpointInfo]()

  def SetBreakpointEnabled(breakpointType: Int, stageId: Int, taskIndex: Long, subIndex: Int, enabled: Boolean) = {
    if (enabled == false) {
      Signal(breakpointType, stageId, taskIndex, subIndex)
    }

    breakpointInfos.synchronized {
      val key = (breakpointType, stageId, taskIndex, subIndex)
      if (breakpointInfos.contains(key)) {
        breakpointInfos(key).SetEnabled(enabled)
      }
      else {
        val newInfo = new BreakpointInfo()
        newInfo.SetEnabled(enabled)
        breakpointInfos(key) = newInfo
      }
    }
  }

  private def Signal(breakpointType: Int, stageId: Int, taskId: Long, subIndex: Int) = {
    var waitObject: Object = null
    waitObjects.synchronized {
      waitObject = waitObjects.getOrElse((breakpointType, stageId, taskId, subIndex), null)
    }
    if (waitObject != null) {
      waitObject.synchronized {
        waitObject.notify()
      }
    }
  }

  def PauseOrExecute(breakpointType: Int, stageId: Int, taskIndex: Long, subIndex: Int): Unit = {
    DebugHelper.log("INFO", "BreakpointManager", s"** PauseOrExecute: $breakpointType, $stageId, $taskIndex, $subIndex **")
    val info: BreakpointInfo = GetBreakpointInfo(breakpointType, stageId, taskIndex, subIndex)
    if (info != null && info.GetEnabled()) {
      DebugHelper.log("INFO", "BreakpointManager", s"** PauseOrExecute: $breakpointType, $stageId, $taskIndex, $subIndex ** -- isEnabled")
      info.SetPaused(true)
      if (breakpointType == BreakpointType.TASK_LEVEL) {
        BreakpointManager.SendBreakpointStatus(stageId, taskIndex, subIndex, true)
      }
      else {
        BreakpointManager.SendSubtaskBreakpointStatus(stageId, taskIndex, subIndex, true)
      }
      DebugHelper.log("INFO", "BreakpointManager", s"** PauseOrExecute: $breakpointType, $stageId, $taskIndex, $subIndex ** -- wait")
      Wait(breakpointType, stageId, taskIndex, subIndex)
      DebugHelper.log("INFO", "BreakpointManager", s"** PauseOrExecute: $breakpointType, $stageId, $taskIndex, $subIndex ** -- resume")
      info.SetPaused(false)
      if (breakpointType == BreakpointType.TASK_LEVEL) {
        BreakpointManager.SendBreakpointStatus(stageId, taskIndex, subIndex, false)
      }
      else {
        BreakpointManager.SendSubtaskBreakpointStatus(stageId, taskIndex, subIndex, false)
      }
    }
  }

  private def GetBreakpointInfo(breakpointType: Int, stageId: Int, taskId: Long, subIndex: Int): BreakpointInfo = {
    lockBreakpointManager.synchronized {
      val key: (Int, Int, Long, Int) = (breakpointType, stageId, taskId, subIndex)
      if (breakpointInfos.contains(key)) {
        val info: BreakpointInfo = breakpointInfos.getOrElse(key, null)
        info
      }
      else {
        null
      }
    }
  }

  private def Wait(breakpointType: Int, stageId: Int, taskId: Long, subIndex: Int) = {
    DebugHelper.log("INFO", "BreakpointManager", s"** WaitStart: $stageId, $taskId **")
    var waitObject: Object = null
    waitObjects.synchronized {
      waitObject = waitObjects.getOrElse((breakpointType, stageId, taskId, subIndex), null)
    }

    if (waitObject != null) {
      waitObject.synchronized {
        waitObject.wait()
      }
    }
    else {
      val newWaitObject = new Object()
      waitObjects.synchronized {
        waitObjects += (((breakpointType, stageId, taskId, subIndex), newWaitObject))
      }
      newWaitObject.synchronized {
        newWaitObject.wait()
      }
    }
    DebugHelper.log("INFO", "BreakpointManager", s"** WaitEnd: $stageId, $taskId **")
  }

  def Resume(stageId: Int, taskIndex: Long) = {
    DebugHelper.log("INFO", "BreakpointManager", s"** Resume: $stageId, $taskIndex **")

    Signal(BreakpointType.TASK_LEVEL, stageId, taskIndex, 0)
    Signal(BreakpointType.TASK_LEVEL, stageId, taskIndex, 1)

    waitObjects.synchronized {
      waitObjects.keys.foreach(e => {
        if (e._1 == BreakpointType.SUBTASK_LEVEL && e._2 == stageId && e._3 == taskIndex) {
          Signal(e._1, e._2, e._3, e._4)
        }
      })
    }
  }

  def ResumeSubtask(stageId: Int, taskIndex: Long, subIndex: Int) = {
    DebugHelper.log("INFO", "BreakpointManager", s"** ResumeSubtask: $stageId, $taskIndex **")
    Signal(BreakpointType.SUBTASK_LEVEL, stageId, taskIndex, subIndex)
  }

  def ResumeAll() = {
    DebugHelper.log("INFO", "BreakpointManager", s"** ResumeAll: **")
    waitObjects.synchronized {
      waitObjects.keys.foreach(e => {
        Signal(e._1, e._2, e._3, e._4)
      })
    }
  }

  def SendBreakpointStatus(stageId: Int, taskId: Long, beforeOrAfter: Int, paused: Boolean) = {
    val driver = ExecutorManager.GetDriver
    if (driver != null) {
      DebugHelper.log("INFO", "DriverActorContainer", s"** SendBreakpointStatus: $stageId, $taskId, $beforeOrAfter, $paused **")
      driver ! BreakpointStatusMessage(stageId, taskId, beforeOrAfter, paused)
    }
    else {
      DebugHelper.log("ERROR", "DriverActorContainer", "driver is null.")
    }
  }

  def SendSubtaskBreakpointStatus(stageId: Int, taskId: Long, subIndex: Int, paused: Boolean) = {
    val driver = ExecutorManager.GetDriver
    if (driver != null) {
      DebugHelper.log("INFO", "DriverActorContainer", s"** SendSubtaskBreakpointStatus: $stageId, $taskId, $subIndex, $paused **")
      driver ! SubtaskBreakpointStatusMessage(stageId, taskId, subIndex, paused)
    }
    else {
      DebugHelper.log("ERROR", "DriverActorContainer", "driver is null.")
    }
  }
}


/** BDD END **/