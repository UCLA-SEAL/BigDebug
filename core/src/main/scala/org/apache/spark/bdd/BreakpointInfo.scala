/** BDD START **/

package org.apache.spark.bdd

class BreakpointInfo {
  // Breakpoint Type
  private var breakpointType: Int = BreakpointType.UNDEFINED

  def GetBreakpointType(): Int = this.breakpointType

  def SetBreakpointType(breakpointType: Int) = {
    this.breakpointType = breakpointType
  }

  // Possible states for task-level and subtask-level breakpoints
  private var enabled: Boolean = false

  def GetEnabled(): Boolean = this.enabled

  def SetEnabled(enabled: Boolean) = {
    this.enabled = enabled
  }

  private var paused: Boolean = false

  def GetPaused(): Boolean = this.paused

  def SetPaused(paused: Boolean) = {
    this.paused = paused
  }

  private var effective: Boolean = false

  def GetEffective(): Boolean = this.effective

  def SetEffective(effective: Boolean) = {
    this.effective = effective
  }

  // Additional Information
  private var description: String = "undefined"

  def GetDescription(): String = this.description

  def SetDescription(description: String) = {
    this.description = description
  }

  // ID to specify the breakpoint location
  // stageId
  // taskId
  // subId - depending on the breakpoint type
  //    SUBTASK_LEVEL -> subIndex, TASK_LEVEL -> 'before' or 'after'
  private var stageId: Int = -1

  def GetStageId(): Int = this.stageId

  def SetStageId(stageId: Int) = {
    this.stageId = stageId
  }

  private var taskId: Long = -1

  def GetTaskId(): Long = this.taskId

  def SetTaskId(taskId: Int) = {
    this.taskId = taskId
  }

  private var subId: Int = -1

  def GetSubId(): Int = this.subId

  def SetSubId(subId: Int) = {
    this.subId = subId
  }
}

/** BDD END **/