package org.apache.spark.ui.debugger

import javax.servlet.http.HttpServletRequest

import org.apache.spark.SparkContext
import org.apache.spark.bdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.ui.{UIUtils, WebUIPage}

import scala.xml.{Node, NodeSeq}


private[ui] class DebuggerPage(parent: DebuggerTab, objectManager: DebuggerObjectManager) extends WebUIPage("") {
  private val listener = parent.listener

  def touchContext = {
    // TODO: Need to be clarified. Messy code.
    //DebugHelper.log(DebugHelper.MessageType.INFO,"touchContext","Begin")
    val sc: SparkContext = parent.getSparkContext
    for (stage <- sc.dagScheduler.getWaitingStages()) {
      val waitingStage = objectManager.addOrGetStageObject(stage.id)
      waitingStage.setCurrentStatus("waiting")
      waitingStage.setName(stage.name)
      waitingStage.setRDDInfo(stage.rdd.toString)
      waitingStage.setRDDDetailedInfo(stage.rdd.toDebugString)

      for (i <- 0 until stage.numTasks) {
        val newChildTask = waitingStage.addOrGetChildTask(i)
        newChildTask.setCurrentStatus("waiting")
        //
        //        DebugHelper.log(DebugHelper.MessageType.INFO,"", "Task " + i)
        //        for(partition <- stage.rdd.partitions){
        //          DebugHelper.log(DebugHelper.MessageType.VERBOSE,"Partition", "index:" +  partition.index + "toString:" + partition.toString)
        //          DebugHelper.log(DebugHelper.MessageType.INFO,"", "Dependencies " + stage.rdd.dependencies.size)
        //          for (dependency <- stage.rdd.dependencies) {
        //            val dummyTaskContext = new TaskContext(stage.id, partition.index, -1, true)
        //            val computeResult = dependency.rdd.compute(partition, dummyTaskContext)
        //            DebugHelper.log(DebugHelper.MessageType.INFO,"Compute Result", "Size:" + computeResult.size)
        //            while(computeResult.hasNext){
        //              println(computeResult.next());
        //            }
        //            DebugHelper.log(DebugHelper.MessageType.VERBOSE,"Partition", dependency.rdd.toDebugString)
        //          }
        //        }
      }

      /*
      println("id:" + stage.id)
      println("name:" + stage.name)
      println("numTasks:" + stage.numTasks)
      println("numPartitions:" + stage.rdd.partitions.size)
      println("rdd:" + stage.rdd.toDebugString)
      println("pendingTasks:" + stage.pendingTasks.size)
      println("latestInfo.details:" + stage.latestInfo.details)

      println("=====================================")
      TaskExecutionManager.printList
      println("=====================================")
      */
    }


    // Update subtask information
    for (stageObject: DebuggerStageObject <- objectManager.stageObjects) {
      val stage = sc.dagScheduler.getStage(stageObject.stageId)
      if (stage != null) {
        // MappedRDD - FlatMappedRDD - README.md - README.md
        //          [1]             [2]         [3]

        // NOTE: What if the RDD is checkpointed?
        for (i <- 0 until stage.numTasks) {
          var dependentRDD: RDD[_] = stage.rdd

          var childTask: DebuggerTaskObject = stageObject.getChildTask(i)
          var looping: Boolean = true
          while (looping) {
            val description: String = dependentRDD.toString

            TaskExecutionManager.SetBreakpointDescription(BreakpointType.SUBTASK_LEVEL, stage.id, i, dependentRDD.id, description)

            childTask.setSubtaskInfo(dependentRDD.id, description)

            if (dependentRDD.dependencies.length == 0) {
              looping = false // exit
            }
            else {
              dependentRDD = dependentRDD.firstParent
            }
          };
        }
      }
      else {
        // DebugHelper.log("ERROR","touchContext","Can't retrieve the stage info: " + stageObject.stageId)
      }
    }


    //DebugHelper.log(DebugHelper.MessageType.INFO,"touchContext","End")
  }

  /**
   * A helper function to return HTML class names
   * @param info
   * @param direction 'left' or 'right'
   * @return
   */
  private def getHtmlClassName(info: BreakpointInfo, direction: String): String = {
    if (info != null) {
      if (info.GetPaused()) "bdd_breakpoint_" + direction + "_red"
      else if (info.GetEnabled()) "bdd_breakpoint_" + direction + "_green"
      else ""
    }
    else ""
  }

  private def getToggleBreakpointUri(info: BreakpointInfo, uri: String): String = {
    if (info != null && info.GetEnabled()) {
      // the breakpoint is already enabled
      uri + "&enabled=0";
    }
    else {
      uri + "&enabled=1";
    }
  }

  def renderTaskObject(taskObject: DebuggerTaskObject): Seq[Node] = {
    val stageId = taskObject.parent.stageId
    val index = taskObject.index

    val breakpointInfoBefore: BreakpointInfo = TaskExecutionManager.AddOrGetBreakpointInfo(BreakpointType.TASK_LEVEL, stageId, index, 0)
    val breakpointInfoAfter: BreakpointInfo = TaskExecutionManager.AddOrGetBreakpointInfo(BreakpointType.TASK_LEVEL, stageId, index, 1)

    val resumeTaskLinkUri = "%s/debugger/do?command=resume&type=%d&stageid=%d&taskindex=%d".format(UIUtils.prependBaseUri(parent.basePath), BreakpointType.TASK_LEVEL, stageId, index)
    val resumeSubtaskLinkUri = "%s/debugger/do?command=resume&type=%d&stageid=%d&taskindex=%d".format(UIUtils.prependBaseUri(parent.basePath), BreakpointType.SUBTASK_LEVEL, stageId, index)

    val toggleBreakpointBeforeUriTemplate = "%s/debugger/do?command=breakpoint&type=%d&stageid=%d&taskindex=%d&subindex=0".format(UIUtils.prependBaseUri(parent.basePath), BreakpointType.TASK_LEVEL, stageId, index);
    val toggleBreakpointBeforeUri = getToggleBreakpointUri(breakpointInfoBefore, toggleBreakpointBeforeUriTemplate)
    val toggleBreakpointAfterUriTemplate = "%s/debugger/do?command=breakpoint&type=%d&stageid=%d&taskindex=%d&subindex=1".format(UIUtils.prependBaseUri(parent.basePath), BreakpointType.TASK_LEVEL, stageId, index)
    val toggleBreakpointAfterUri = getToggleBreakpointUri(breakpointInfoAfter, toggleBreakpointAfterUriTemplate)
    val toggleSubtaskBreakpointBaseUri = "%s/debugger/do?command=breakpoint&type=%d&stageid=%d&taskindex=%d".format(UIUtils.prependBaseUri(parent.basePath), BreakpointType.SUBTASK_LEVEL, stageId, index)

    val classForBreakpointBeforeRunning = getHtmlClassName(breakpointInfoBefore, "left")
    val classForBreakpointAfterRunning = getHtmlClassName(breakpointInfoAfter, "right")

    // TODO: Instead of using tuple, let's make an object that contains the relevant data.
    // TODO: (stage, partitionId) should be the key. Now, it returns the entire list, therefore it causes redundant computation.
    // tuples(Key, Value, stageID, PartitionID)
    //DebugHelper.log(DebugHelper.MessageType.INFO,"renderTaskObject","taskObject.getTaskType=" + taskObject.getTaskType)
    val getResultOutputFromTask: List[(Any, Any, Int, Int)] = taskObject.getTaskType match {
      case "ResultTask" => TaskExecutionManager.getResultOutput()
      //   case "ShuffleMapTask" => TaskExecutionManager.getWatchPointList()
      case _ => List[(Any, Any, Int, Int)]()
    }
    val relevantResultOutput = getResultOutputFromTask.filter(e => e._3 == stageId && e._4 == index)
    //DebugHelper.log(DebugHelper.MessageType.INFO,"debuggerPage",relevantResultOutput.size)
    val resultFromTask = relevantResultOutput.map(e => {
      "(" + e._1 + "," + e._2 + ")"
    }).mkString("\n")
    //DebugHelper.log(DebugHelper.MessageType.INFO,"debuggerPage-resultFromTask",resultFromTask)

    val subtaskInfoHtml: Seq[Node] = {
      var temp = List[Node]()
      for (x: (Int, String) <- taskObject.getSubtaskInfo) {
        val subtaskBreakpointInfo: BreakpointInfo = TaskExecutionManager.AddOrGetBreakpointInfo(BreakpointType.SUBTASK_LEVEL, stageId, index, x._1)
        var msg = "Set"

        if (subtaskBreakpointInfo != null) {
          if (subtaskBreakpointInfo.GetEnabled())
            msg = "Unset"

          if (subtaskBreakpointInfo.GetPaused())
            msg = msg + "@"
        }

        temp = temp :+ <span>
          <p>
            {x._2}
          </p> <a href={toggleSubtaskBreakpointBaseUri + "&subindex=%d".format(x._1)}>
            {msg}
          </a>
        </span>
      }
      temp
    }
    val classForTaskDiv = Array("bdd_task", classForBreakpointBeforeRunning, classForBreakpointAfterRunning).mkString(" ")
    <div class={classForTaskDiv}>
      <div>
        <strong>Task(
          {taskObject.index}
          )</strong> <span>(
        {taskObject.getTaskType}
        ) -
        {taskObject.getCurrentStatus}
      </span>
      </div>
      <div>
        <pre>
          {taskObject.getDetailedInfo}
        </pre>
      </div>
      <div>
        <a class="btn btn-mini btn-success" type="button" href={resumeTaskLinkUri}>Resume</a>
        <span>Set a breakpoint</span>
        <a class="btn btn-mini btn-default" type="button" href={toggleBreakpointBeforeUri}>Before</a>
        <a class="btn btn-mini btn-default" type="button" href={toggleBreakpointAfterUri}>After</a>
        <div>
          <span>Subtask Breakpoints</span>
          <div>
            {subtaskInfoHtml}
          </div>
        </div>
      </div>
      <pre class="bdd_result_panel">
        {resultFromTask}
      </pre>
    </div>
  }

  def renderStageObject(stageObject: DebuggerStageObject): Seq[Node] = {
    <div class="bdd_panel">
      <div>
        <strong>Stage[
          {stageObject.stageId}
          ]:
          {stageObject.getName}
        </strong>
      </div>
      <div>
        {stageObject.getRDDInfo}
      </div>
      <div>
        Status:
        {stageObject.getCurrentStatus}
      </div>
      <div>
        {stageObject.getChildTasks.map(renderTaskObject)}
      </div>
      <div>
        <string>RDD</string>
        <div>
          <pre>
            {stageObject.getRDDDetailedInfo}
          </pre>
        </div>
      </div>
    </div>
  }

  def renderedTaskProfiling(): Seq[Node] = {
    val title = "Task Execution Time  "
    val content =
      <div>
        <br/>
        <br/>
        <br/>
        <div id="chartContainer" style="height: 400px; width: 700px;">
        </div>
      </div>
    content
  }

  def renderWatchpointObject(): Seq[Node] = {
    val watchpoitnKeys = TaskExecutionManager.getWPKeys
    def dropRow(wp: (Int, Int)) = Seq[Node] {
      val dumpWP = "%s/debugger/".format(UIUtils.prependBaseUri(parent.basePath)) + "watchpoint?id=" + wp._2.toString + "&tid=" +
        wp._1.toString
      <li>
        <a href={dumpWP}>Dump Watch Point
          {wp._2.toString}
          From Tash
          {wp._1.toString}
        </a>
      </li>
    }
    <div>
      <div class="dropdown">
        <button class="btn btn-success dropdown-toggle" type="button" data-toggle="dropdown">Select WatchPoint RDD to See Tapped Data
          <span class="caret"></span>
        </button>
        <ul class="dropdown-menu">
          {watchpoitnKeys.map(r => dropRow(r))}
        </ul>
      </div>
    </div>

  }

  def getLineNumber(str: String): String = {
    if (str.contains("scala:")) {
      val start = str.indexOf("scala:")
      val v = str.substring(start + 6, str.length)
      return v.trim()
    } else {
      return "0"
    }
  }


  def getCrashLineNumber(): String = {
    val seq = TaskExecutionManager.crashedRDDList()
    var str = ""
    for (rdd <- seq) {
      val rdd_str = TaskExecutionManager.getRDDDetails(rdd)
      val start = rdd_str.indexOf("scala:")
      val v = rdd_str.substring(start + 6, rdd_str.length)
      str = str + v.trim() + " "
    }
    return str
  }


    def render(request: HttpServletRequest): Seq[Node] = {
      listener.synchronized {
        touchContext

        val doUrl = "%s/debugger/do".format(UIUtils.prependBaseUri(parent.basePath))
        val terminateLinkUri = doUrl + "?command=terminate"
        val pauseAllLinkUri = doUrl + "?command=pause_all"
        val resumeAllLinkUri = doUrl + "?command=resume_all"
        var dumpIntraTaskLinkUri = doUrl + "?command=dump_intratask"
        var testLinkUri = doUrl + "?command=test"
        var crashSkip = doUrl + "?command=skip"
        var simBP = doUrl + "?command=killandrun"


        val resume = doUrl + "?command=resume"

        val stepover = doUrl + "?command=stepover"


  //      var dumpWP = "%s/debugger/".format(UIUtils.prependBaseUri(parent.basePath)) + "watchpoint"
//        val renderedContentForStages = objectManager.renderContentForStages(renderStageObject)

       // val regex_prev = TaskExecutionManager.getExpression
       // val current_Crash: Tuple4[String, Int, Int, Int] = TaskExecutionManager.getCurrentCrashCulprit
        //  println(current_Crash.toString())
       // val path_d3_js = UIUtils.prependBaseUri("/static/d3.min.js")
       // val path_debugger_js = UIUtils.prependBaseUri("/static/bdd/js/debugger.js")


        val path_dag_viz = UIUtils.prependBaseUri("/static/spark-dag-viz.js")
        val fileContent = scala.io.Source.fromFile(parent.getSparkContext.getBigDebugConfiguration().SOURCECODE_PATH).mkString
        val filename = parent.getSparkContext.getBigDebugConfiguration().SOURCECODE_PATH.split("/").last


        //  println(current_Crash)
        val operationGraphListener = parent.operationGraphListener
        var content: NodeSeq =
          <div>
            <div id="container2">
              <div id="container1">
                <div id="col1">

                  <!--      <script src={path_d3_js} charset="utf-8"></script>
          <script src={path_debugger_js} charset="utf-8"></script>
          <script src={path_dag_viz} charset="utf-8"></script>
-->

                  <div class="well well-large" style="text-align:center;border:1px solid red">
                    <h4>Breakpoint Controls</h4>
                    <div>
                      <a class="btn  btn-success" type="button" href={resume}>Resume</a>
                      <a class="btn btn-success" type="button" href={stepover}>Step Over</a>
                    </div>
                    <br/>
                    <div>
                      <input type="hidden" id="crashlineinfo" value={getCrashLineNumber()}></input>
                      <input type="hidden" id="breaklineinfo" value={getLineNumber(TaskExecutionManager.getCurrentBreakpointRDDDetails())}></input> <h4>Current Breakpoint location is after the
                      {TaskExecutionManager.getCurrentBreakpointRDDDetails()}
                    </h4>
                    </div>
                  </div>
                  <br/>
                  <br/>
                  <div>
                    {UIUtils.showDagVizForJob(
                    0, operationGraphListener.getOperationGraphForJob(0))}
                  </div>
                  <input type="hidden" id="websocketport" name="portws"
                         value={parent.getDriverWebSocketPort}/>
                </div>

                <div id="col2">
                  <div>
                    <h3>{filename}</h3>
                  </div>
                  <div>

                    <textarea id="code" name="code" style="display: none;">
                      {fileContent}
                    </textarea>
                  </div>
                </div>

              </div>
            </div>
            <h3>Task Level Latency</h3>
          </div>

        content = content ++ renderedTaskProfiling
        content = content ++ <div class="well well-large" style="text-align:center;border:1px solid red">
          <br/>
          <h4>Dump WatchPoints</h4>{renderWatchpointObject}
        </div>

        val wstype = 2
        UIUtils.headerSparkPage("Debugger", content, parent, onload = s"createCode();initWebSocket($wstype);chartRender()")
      }
    }
  }