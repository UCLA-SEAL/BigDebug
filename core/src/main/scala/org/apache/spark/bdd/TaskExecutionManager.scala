package org.apache.spark.bdd

import scala.collection.mutable._

/**
 * Created by noel on 1/11/15.
 */
object TaskExecutionManager {
  def isFirst(stageId: Int): Boolean = {
    veryFirstStageId.synchronized {
      if (veryFirstStageId.isEmpty) {
        veryFirstStageId = Option[Int](stageId)
      }
      veryFirstStageId.getOrElse(-1) == stageId
    }
  }

  private var veryFirstStageId: Option[Int] = None

  def ToggleBreakpointAfterRunning(stageId: Int, index: Long) = {
    SetBreakpointAfterRunning(stageId, index, !IsBreakpointAfterRunningEnabled(stageId, index))
  }

  def ToggleBreakpointBeforeRunning(stageId: Int, index: Long) = {
    SetBreakpointBeforeRunning(stageId, index, !IsBreakpointBeforeRunningEnabled(stageId, index))
  }

  private val breakpointsBeforeRunning = HashMap[(Int, Long), Boolean]()
  private val breakpointsAfterRunning = HashMap[(Int, Long), Boolean]()

  private val isWaitingBeforeRunning = HashMap[(Int, Long), Boolean]()
  private val isWaitingAfterRunning = HashMap[(Int, Long), Boolean]()

  def SetBreakpointBeforeRunning(stageId: Int, index: Long, enabled: Boolean): Unit = {
    if (enabled == false) {
      SignalImpl(waitObjectsBeforeRunning, stageId, index)
    }
    //println("*** set breakpoint before running *** " + (stageId, index).toString() + ":" + enabled)
    breakpointsBeforeRunning.synchronized {
      breakpointsBeforeRunning += ((stageId, index) -> enabled)
    }
  }

  def SetBreakpointAfterRunning(stageId: Int, index: Long, enabled: Boolean): Unit = {
    if (enabled == false) {
      SignalImpl(waitObjectsAfterRunning, stageId, index)
    }
    //println("*** set breakpoint after running *** " + (stageId, index).toString() + ":" + enabled)
    breakpointsAfterRunning.synchronized {
      breakpointsAfterRunning += ((stageId, index) -> enabled)
    }
  }

  def IsBreakpointBeforeRunningEnabled(stageId: Int, index: Long): Boolean = {
    breakpointsBeforeRunning.synchronized {
      breakpointsBeforeRunning.getOrElse((stageId, index), false)
    }
  }

  def IsBreakpointAfterRunningEnabled(stageId: Int, index: Long): Boolean = {
    breakpointsAfterRunning.synchronized {
      breakpointsAfterRunning.getOrElse((stageId, index), false)
    }
  }

  def IsWaitingBeforeRunning(stageId: Int, index: Long) = {
    isWaitingBeforeRunning.synchronized {
      isWaitingBeforeRunning.getOrElse((stageId, index), false)
    }
  }

  def IsWaitingAfterRunning(stageId: Int, index: Long) = {
    isWaitingAfterRunning.synchronized {
      isWaitingAfterRunning.getOrElse((stageId, index), false)
    }
  }

  // (Int, Long) = (stageId, index -- maybe partitionId)
  private val waitObjectsBeforeRunning = HashMap[(Int, Long), Object]()
  private val waitObjectsAfterRunning = HashMap[(Int, Long), Object]()

  def CheckBreakPointBeforeRunning(stageId: Int, index: Long): Unit = {
    if (IsBreakpointBeforeRunningEnabled(stageId, index)) {
      isWaitingBeforeRunning.synchronized {
        isWaitingBeforeRunning += ((stageId, index) -> true)
      }
      WaitImpl(waitObjectsBeforeRunning, stageId, index)
      isWaitingBeforeRunning.synchronized {
        isWaitingBeforeRunning += ((stageId, index) -> false)
      }
    }
    else {
      println("$$ breakpointBeforeRunning is not set $$ " + stageId + "." + index)
    }
  }

  def CheckBreakPointAfterRunning(stageId: Int, index: Long): Unit = {
    if (IsBreakpointAfterRunningEnabled(stageId, index)) {
      isWaitingAfterRunning.synchronized {
        isWaitingAfterRunning += ((stageId, index) -> true)
      }
      WaitImpl(waitObjectsAfterRunning, stageId, index)
      isWaitingAfterRunning.synchronized {
        isWaitingAfterRunning += ((stageId, index) -> false)
      }
    }
    else {
      println("$$ breakpointAfterRunning is not set $$ " + stageId + "." + index)
    }
  }

  def Resume(stageId: Int, index: Long) = {
    SignalImpl(waitObjectsBeforeRunning, stageId, index)
    SignalImpl(waitObjectsAfterRunning, stageId, index)
  }

  def ResumeAll() = {
    waitObjectsBeforeRunning.synchronized {
      waitObjectsBeforeRunning.keys.foreach(e => SignalImpl(waitObjectsBeforeRunning, e._1, e._2))
    }
    waitObjectsAfterRunning.synchronized {
      waitObjectsAfterRunning.keys.foreach(e => SignalImpl(waitObjectsAfterRunning, e._1, e._2))
    }
  }

  private def WaitImpl(container: HashMap[(Int, Long), Object], stageId: Int, index: Long): Unit = {
    var waitObject:Object = null
    container.synchronized{
      waitObject = container.getOrElse((stageId, index), null)
    }
    if (waitObject != null) {
      waitObject.synchronized {
        //        println("*** waiting *** " + (stageId, index, Thread.currentThread().getId).toString())
        waitObject.wait() // TODO: it is not a sophisticated way of waiting
        //        println("*** notified *** " + (stageId, index, Thread.currentThread().getId).toString())
      }
    }
    else {
      // We have to add a wait object into the container.
      val newWaitObject = new Object()
      container.synchronized {
        container += ((stageId, index) -> newWaitObject)
      }
      newWaitObject.synchronized {
        //        println("*** waiting[new] *** " + (stageId, index, Thread.currentThread().getId).toString())
        newWaitObject.wait()
        //        println("*** notified[new] *** " + (stageId, index, Thread.currentThread().getId).toString())
      }
    }
  }

  private def SignalImpl(container: HashMap[(Int, Long), Object], stageId: Int, index: Long): Unit = {
    var waitObject:Object = null
    container.synchronized{
      waitObject = container.getOrElse((stageId, index), null)
    }
    if (waitObject != null) {
      waitObject.synchronized {
        //        println("*** notifying *** " + (stageId, index, Thread.currentThread().getId).toString())
        waitObject.notify()
      }
    }
  }
}
