package org.apache.spark.ui.debugger

import javax.servlet.http.HttpServletRequest
import org.apache.spark.SparkContext

import scala.xml.Node
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.bdd._

private[ui] class DebuggerPage(parent: DebuggerTab, objectManager:DebuggerObjectManager) extends WebUIPage("") {
  private val listener = parent.listener

  def touchContext = {

    println("====== [touchContext] ======")
    val sc:SparkContext = parent.getSparkContext
    for(stage<- sc.dagScheduler.getWaitingStages()){
      val waitingStage = objectManager.addOrGetStageObject(stage.id)
      waitingStage.setCurrentStatus("waiting")
      waitingStage.setName(stage.name)
      waitingStage.setRDDInfo(stage.rdd.toString)
      waitingStage.setRDDDetailedInfo(stage.rdd.toDebugString)
      for(i<-0 until stage.numTasks){
        val newChildTask = waitingStage.addOrGetChildTask(i)
        newChildTask.setCurrentStatus("waiting")
        //newChildTask.setRDDName(stage.rdd.toString)
      }

      println("id:" + stage.id)
      println("name:" + stage.name)
      println("numTasks:" + stage.numTasks)
      println("numPartitions:" + stage.rdd.partitions.size)
      println("rdd:" + stage.rdd.toDebugString)
      println("pendingTasks:" + stage.pendingTasks.size)
      println("latestInfo.details:" + stage.latestInfo.details)

    }
//    for (rddInfo <- sc.getRDDStorageInfo){
//      println(rddInfo.name)
//    }
    println("============================")
  }

  def renderTaskObject(taskObject: DebuggerTaskObject): Seq[Node] = {
    val stageId = taskObject.parent.stageId
    val index = taskObject.index
    val resumeTaskLinkUri = "%s/debugger/do?command=resume_task&stage_id=%d&index=%d".format(UIUtils.prependBaseUri(parent.basePath), stageId, index)
    val setBreakpointBeforeLinkUri = "%s/debugger/do?command=toggle_breakpoint_before&stage_id=%d&index=%d".format(UIUtils.prependBaseUri(parent.basePath), stageId, index)
    val setBreakpointAfterLinkUri = "%s/debugger/do?command=toggle_breakpoint_after&stage_id=%d&index=%d".format(UIUtils.prependBaseUri(parent.basePath), stageId, index)

    val breakpointBeforeRunningEnabled = TaskExecutionManager.IsBreakpointBeforeRunningEnabled(stageId, index)
    val breakpointAfterRunningEnabled = TaskExecutionManager.IsBreakpointAfterRunningEnabled(stageId, index)
    val breakpointBeforeRunningWaiting = TaskExecutionManager.IsWaitingBeforeRunning(stageId, index)
    val breakpointAfterRunningWaiting = TaskExecutionManager.IsWaitingAfterRunning(stageId, index)

    val classForBreakpointBeforeRunning =
      if (breakpointBeforeRunningWaiting) "bdd_breakpoint_left_red"
      else if (breakpointBeforeRunningEnabled) "bdd_breakpoint_left_green"
      else ""

    val classForBreakpointAfterRunning =
      if (breakpointAfterRunningWaiting) "bdd_breakpoint_right_red"
      else if (breakpointAfterRunningEnabled) "bdd_breakpoint_right_green"
      else ""

    val classForTaskDiv = Array("bdd_task", classForBreakpointBeforeRunning, classForBreakpointAfterRunning).mkString(" ")
    <div class={classForTaskDiv}>
      <div>
        <strong>Task({taskObject.index})</strong><span> ({taskObject.getTaskType}) - {taskObject.getCurrentStatus}</span>
      </div>
      <div><pre>{taskObject.getDetailedInfo}</pre></div>
      <div>
        <a class="btn btn-mini btn-success" type="button" href={resumeTaskLinkUri}>Resume</a>
        <span>Set a breakpoint </span>
        <a class="btn btn-mini btn-default" type="button" href={setBreakpointBeforeLinkUri}>Before</a>
        <a class="btn btn-mini btn-default" type="button" href={setBreakpointAfterLinkUri}>After</a>
      </div>
    </div>
  }

  def renderStageObject(stageObject: DebuggerStageObject): Seq[Node] = {
    <div class="bdd_panel">
      <div>
        <strong>Stage[{stageObject.stageId}]:{stageObject.getName}</strong>
      </div>
      <div>
        {stageObject.getRDDInfo}
      </div>
      <div>
        Status:{stageObject.getCurrentStatus}
      </div>
      <div>
        {stageObject.getChildTasks.map(renderTaskObject)}
      </div>
      <div>
        <string>RDD</string>
        <div><pre>{stageObject.getRDDDetailedInfo}</pre></div>
      </div>
    </div>
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    listener.synchronized {
      touchContext

      val terminateLinkUri = "%s/debugger/do?command=terminate".format(UIUtils.prependBaseUri(parent.basePath))
      val pauseAllLinkUri = "%s/debugger/do?command=pause_all".format(UIUtils.prependBaseUri(parent.basePath))
      val resumeAllLinkUri = "%s/debugger/do?command=resume_all".format(UIUtils.prependBaseUri(parent.basePath))
      val renderedContentForStages = objectManager.renderContentForStages(renderStageObject)
      val content =
      <div>
        <div>
          <span>Global Commands: </span>
          <a href={pauseAllLinkUri}>PauseAll</a>,
          <a href={resumeAllLinkUri}>ResumeAll</a>,
          <a href={terminateLinkUri}>Terminate</a>
        </div>
        <div>
          {renderedContentForStages}
        </div>
        <h3>Log Messages</h3>
      </div>

      UIUtils.headerSparkPage("Debugger", content, parent)
    }
  }
}
