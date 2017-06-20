package org.apache.spark.bdd

object StatusSynchronization {
  private val EVENT_STAGE_COMPLETED: Int = 0
  private val EVENT_STAGE_SUBMITTED: Int = 1
  private val EVENT_TASK_SUBMITTED: Int = 2
  private val EVENT_TASK_STARTED: Int = 3
  private val EVENT_TASK_END: Int = 4

  private val infoManager = new DebuggerInfoManager()

  private var logicalClock: Int = 0
  private var lockLogicalClock = new Object()
  private var socket: Socket = null

  def SendMessageToUI( message: String ) = {
    if ( socket == null ){
      val size = message.length
      DebugHelper.log( "WARNING", "SendMessageToUI", s"The Socket.io connection hasn't established. The message (size: $size) is dropped." )
    }
    else {
      socket.emit( "messageToUI", message )
    }
  }

  def GetLogicalClock: Int = {
    lockLogicalClock.synchronized {
      logicalClock
    }
  }

  def IncAndGetLogicalClock: Int = {
    var copiedLogicalClock: Int = 0
    lockLogicalClock.synchronized {
      logicalClock = logicalClock + 1
      copiedLogicalClock = logicalClock
    }
    copiedLogicalClock
  }

  def HandleUICommand(objFromUI: Map[String, AnyRef], socket: Socket): Unit = {
    if (objFromUI != null) {
      val commandType: String = objFromUI("type").asInstanceOf[String]

      DebugHelper.log("INFO", "HandleUICommand", s"commandType: $commandType")

      commandType match {
        case "getStatus" => {
          // To get the current status of Spark program execution.
          val status = infoManager.getStatus
          val lastLogicalClock = infoManager.getLastLogicalClock

          SendMessageToUI(
            s"""
               |{
               |"type": "getStatus",
               |"lastLogicalClock": $lastLogicalClock,
                                                       |"status": $status
                |}
                          """.stripMargin
          )
        }
        case "getBreakpointStatus" => {
          val status = BreakpointManager.getStatus

          SendMessageToUI(
            s"""|
               |{
               |"type": "getBreakpointStatus",
               |"status": $status
                |}
                          """.stripMargin
          )
        }
        case "getWatchpointStatus" => {
          val status = WatchpointManager.getStatus
          SendMessageToUI(
            s"""
               |{
               |"type": "getWatchpointStatus",
               |"status": $status
                |}
                      """.stripMargin)
        }
        case "terminate" => {
          UserInterfaceSupport.Terminate()
        }
        case "pauseAll" => {
        }
        case "resumeAll" => {
          TaskExecutionManager.BroadcastBreakpointResumeAll()
        }
        case "resume" => {
          val breakpointType: Int = objFromUI("type").asInstanceOf[String].toInt
          val stageId: Int = objFromUI("stageId").asInstanceOf[String].toInt
          val taskIndex: Int = objFromUI("taskIndex").asInstanceOf[String].toInt
          val subIndex: Int = objFromUI("subIndex").asInstanceOf[String].toInt
          if (stageId != -1 && taskIndex != -1) {
            val stageObject: DebuggerStageObject = infoManager.getStageObject(stageId)
            val taskObject: DebuggerTaskObject = stageObject.getChildTask(taskIndex)
            val executorId: String = taskObject.getExecutorId()
            TaskExecutionManager.SendBreakpointResume(executorId, breakpointType, stageId, taskIndex, subIndex)
          }
          else {
            DebugHelper.log("ERROR", "handleDebuggerCommand", "Invalid Parameters")
          }
        }
        case "setRegex" => {
          val regex: String = objFromUI("regex").asInstanceOf[String]
          TaskExecutionManager.setExpression(regex)
        }
        case "breakpoint" => {
          /*
           Required parameters

           type   0 : a subtask level breakpoint, 1: a task-level breakpoint (before/ after execution)
           stageid
           taskindex
           subindex
           enabled
           */

          //val executorId:String = objectManager.getStageObject(1).getChildTask(0).getExecutorId
          //DriverActorContainerForMaster.SendSetBreakpointMessage(executorId, 1,0,1,true)

          /**
           * type
           * 0 : a subtask-level breakpoint
           * 1 : a task-level breakpoint (before/ after the task execution)
           * ...
           */
          val breakpointType: Int = objFromUI("type").asInstanceOf[String].toInt
          if (breakpointType == 0 || breakpointType == 1) {
            val stageId: Int = objFromUI("stageId").asInstanceOf[String].toInt
            val taskIndex: Int = objFromUI("taskIndex").asInstanceOf[String].toInt
            val subIndex: Int = objFromUI("subIndex").asInstanceOf[String].toInt

            val enabledTemp: Int = objFromUI("enabled").asInstanceOf[String].toInt

            if (enabledTemp != -1) {
              val enabled: Boolean = if (enabledTemp == 0) false else true

              if (stageId != -1 && taskIndex != -1) {
                val stageObject: DebuggerStageObject = infoManager.getStageObject(stageId)
                val taskObject: DebuggerTaskObject = stageObject.getChildTask(taskIndex)
                val executorId: String = taskObject.getExecutorId()

                if (breakpointType == 0) {
                  // subtask-level
                  TaskExecutionManager.SendSetSubtaskBreakpointMessage(executorId, stageId, taskIndex, subIndex, enabled)
                }
                else if (breakpointType == 1) {
                  // task level
                  TaskExecutionManager.SendSetBreakpointMessage(executorId, stageId, taskIndex, subIndex, enabled)
                }
              }
              else {
                DebugHelper.log("ERROR", "handleDebuggerCommand", s"Invalid breakpoint parameters, (stageId, index) = (" + stageId + ", " + taskIndex + ")")
              }
            }
            else {
              DebugHelper.log("ERROR", "handleDebuggerCommand", "Invalid breakpoint parameter: unspecified 'enabled'")
            }
          }
          else {
            DebugHelper.log("ERROR", "handleDebuggerCommand", s"setBreakpoint:invalidType:" + breakpointType)
          }
        }
        case "hello" => {
          SendMessageToUI(
            """
              |{
              |  "type": "helloConfirmed",
              |  "protocol": "spark",
              |  "version": 1
              |}
            """.stripMargin)
        }
        case _ => {
          // an unhandled command, raise an error message
        }
      }
    }
  }

  def RunService(): Unit = {
    DebugHelper.log("INFO", "StatusSynchronization", "Initialized")
    socket = IO.socket(Configuration.STATUS_SERVER)
    socket.on(Socket.EVENT_CONNECT, new Listener {
      override def call(objects: AnyRef*): Unit = {
        DebugHelper.log("INFO", "StatusSynchronization", "Connected")
      }
    }).on("messageFromUI", new Listener {
      override def call(objects: AnyRef*): Unit = {
        val rawStringFromUI: String = objects(0).toString()
        DebugHelper.log("INFO", "StatusSynchronization", "Message Recevied: " + rawStringFromUI)
        val jsonObjFromUI = scala.util.parsing.json.JSON.parseFull(rawStringFromUI).getOrElse(null)
        if (jsonObjFromUI != null) {
          val objFromUI = jsonObjFromUI.asInstanceOf[Map[String, AnyRef]]
          HandleUICommand(objFromUI, socket)
        }
      }
    }).on(Socket.EVENT_DISCONNECT, new Listener {
      override def call(objects: AnyRef*): Unit = {
        DebugHelper.log("INFO", "StatusSynchronization", "Disconnected")
      }
    })

    socket.connect()
    UserInterfaceSupport.Wait()
    DebugHelper.log("INFO", "StatusSynchronization", "Terminated")
  }


  def OnStageCompleted(stageId: Int) = {
    val clock: Int = IncAndGetLogicalClock
    val stageObject: DebuggerStageObject = infoManager.addOrGetStageObject(stageId)
    stageObject.setCurrentStatus("completed")
    infoManager.updateLastLogicalClock(clock)

    DebugHelper.log("INFO", "OnStageCompleted",
      s"""
         |$stageId is completed
       """.stripMargin)

    SendMessageToUI(
      s"""
         |{
         |"type": "setStageStatus",
         |"clock": $clock,
                           |"stageId": $stageId,
                                                 |"event": "completed"
                                                 |}
                """.stripMargin)
  }

  def OnStageSubmitted(stageId: Int) = {
    val clock: Int = IncAndGetLogicalClock
    val stageObject: DebuggerStageObject = infoManager.addOrGetStageObject(stageId)
    stageObject.setCurrentStatus("submitted")
    infoManager.updateLastLogicalClock(clock)

    DebugHelper.log("INFO", "OnStageSubmitted",
      s"""
         |$stageId is submitted
       """.stripMargin)

    SendMessageToUI(
      s"""
         |{
         |"type": "setStageStatus",
         |"clock": $clock,
                           |"stageId": $stageId,
                                                 |"event": "submitted"
                                                 |}
                """.stripMargin)
  }

  def OnTaskSubmitted(taskIndex: Int) = {
    // do nothing right now
  }

  def OnTaskStarted(stageId: Int, taskIndex: Int) = {
    val clock: Int = IncAndGetLogicalClock

    val stageObject: DebuggerStageObject = infoManager.addOrGetStageObject(stageId)
    val taskObject: DebuggerTaskObject = stageObject.addOrGetChildTask(taskIndex)
    taskObject.setCurrentStatus("started")

    infoManager.updateLastLogicalClock(clock)


    SendMessageToUI(
      s"""
         |{
         |"type": "setTaskStatus",
         |"clock": $clock,
                           |"stageId": $stageId,
                                                 |"taskIndex": $taskIndex,
                                                                           |"event": "started"
                                                                           |}
                """.stripMargin)
  }

  def OnTaskEnd(stageId: Int, taskIndex: Int) = {
    val clock: Int = IncAndGetLogicalClock

    val stageObject: DebuggerStageObject = infoManager.addOrGetStageObject(stageId)
    val taskObject: DebuggerTaskObject = stageObject.addOrGetChildTask(taskIndex)
    taskObject.setCurrentStatus("end")

    infoManager.updateLastLogicalClock(clock)

    SendMessageToUI(
      s"""
         |{
         |"type": "setTaskStatus",
         |"clock": $clock,
                           |"stageId": $stageId,
                                                 |"taskIndex": $taskIndex,
                                                                           |"event": "end"
                                                                           |}
                """.stripMargin)
  }

  def UpdateStageName(stageId: Int, stageName: String): Unit = {
    val clock: Int = IncAndGetLogicalClock

    val stageObject = infoManager.getStageObject(stageId)
    stageObject.setName(stageName)

    SendMessageToUI(
      s"""
         |{
         |"type": "updateStageName",
         |"clock": $clock,
                           |"stageId": $stageId,
                                                 |"stageName": "$stageName"
                                                                            |}
    """.stripMargin)
  }

  def UpdateStageRDD(stageId: Int, stageRDD: String, stageRDDInfo: String) = {
    val clock: Int = IncAndGetLogicalClock

    val stageObject = infoManager.getStageObject(stageId)
    stageObject.setRDDInfo(stageRDD)
    stageObject.setRDDDetailedInfo(stageRDDInfo)

    val escapedStageRDDInfo = stageRDDInfo.replace("\n", "\\n")

    SendMessageToUI(
      s"""
         |{
         |"type": "updateStageRDD",
         |"clock": $clock,
                           |"stageId": $stageId,
                                                 |"stageRDD": "$stageRDD",
                                                                          |"stageRDDInfo": "$escapedStageRDDInfo"
                                                                                                                  |}
    """.stripMargin)
  }

  def UpdateTaskInfo(stageId: Int, taskIndex: Int, taskType: String, taskDetailInfo: String, executorId: String): Unit = {
    val clock: Int = IncAndGetLogicalClock

    val stageObject = infoManager.getStageObject(stageId)
    val taskObject = stageObject.getChildTask(taskIndex)
    taskObject.setTaskType(taskType)
    taskObject.setDetailedInfo(taskDetailInfo)
    taskObject.setExecutorId(executorId)

    SendMessageToUI(
      s"""
         |{
         |"type": "updateTaskInfo",
         |"clock": $clock,
                           |"stageId": $stageId,
                                                 |"taskIndex": $taskIndex,
                                                                           |"taskType": "$taskType",
                                                                                                    |"taskDetailInfo": "$taskDetailInfo",
                                                                                                                                         |"executorId": "$executorId"
                                                                                                                                                                      |}
    """.stripMargin)
  }

  def UpdateSubtaskInfo(stageId:Int, taskIndex:Int, subtaskIndex: Int, description: String) = {
    val clock: Int = IncAndGetLogicalClock

    val stageObject = infoManager.getStageObject(stageId)
    val taskObject = stageObject.addOrGetChildTask(taskIndex)
    taskObject.setSubtaskInfo(subtaskIndex, description)

    SendMessageToUI(
      s"""
         |{
         |"type": "updateSubtaskInfo",
         |"clock": $clock,
                           |"stageId": $stageId,
                                                 |"taskIndex": $taskIndex,
                                                                           |"subtaskIndex": $subtaskIndex,
                                                                                                           |"description": "$description"
                                                                                                                                          |}
    """.stripMargin)
  }
}
