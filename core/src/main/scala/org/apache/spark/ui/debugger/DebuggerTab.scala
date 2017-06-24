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
import org.apache.spark.bdd._
import org.apache.spark.ui._

class DebuggerTab( val listener: DebuggerListener , parent: SparkUI) extends SparkUITab(parent, "debugger") {


  val operationGraphListener = parent.operationGraphListener

  attachPage(new DebuggerPage(this))
  attachPage(new WatchpointPage(this))
  attachPage(new RDDPage(this))
  attachPage(new RDDStPage(this))

  def initiate(): Unit ={
    TaskExecutionManager.setSparkUI(parent)
    parent.attachHandler(JettyUtils.createRedirectHandler("/debugger/do", "/debugger", this.handleDebuggerCommand))
    parent.attachTab(this)
  }

  initiate()

  def getSparkContext: SparkContext = {
    parent.sc.get
  }

  def getDriverWebSocketPort: String = {
    parent.driverWebSocket.getCurrentPort().toString
  }

  def handleDebuggerCommand(request: HttpServletRequest): Unit = {
    val command: String = Option(request.getParameter("command")).getOrElse("")
    command match {
      case "terminate" =>
        parent.shouldBeTerminated = true
      case "killandrun" =>
        println(" In Kill and RUn")
        TaskExecutionManager.startNewjob()
      case "resume" =>
        TaskExecutionManager.resume()
      case "stepover" =>
        TaskExecutionManager.stepOver()
      case "setnewrddfunc" =>
        val code = Option(request.getParameter("code")).getOrElse("")
        val rddid = Option(request.getParameter("rddid")).getOrElse("").toInt
        TaskExecutionManager.resetCode(rddid,code )
      case "batchmodifyfunction" => /***05/12*/
        val code = Option(request.getParameter("code")).getOrElse("")
        val rddid = Option(request.getParameter("rddid")).getOrElse("").toInt
        ///println(s" Batch Code \n\n $code \n\n\n  $rddid")
        TaskExecutionManager.batchModifyCrashRecords(rddid,code)
      case "dump_intratask" =>
        TaskExecutionManager.dumpWatchpointData()
      case "resolve" =>
        val crash: String = Option(request.getParameter("record")).getOrElse("")
        val stage: Int = Option(request.getParameter("stage")).getOrElse("").toInt
        val task: Int = Option(request.getParameter("task")).getOrElse("").toInt
        val subtask: Int = Option(request.getParameter("subtask")).getOrElse("").toInt
        val srnum: Int = Option(request.getParameter("srnum")).getOrElse("").toInt
        //   TaskExecutionManager.simulateCrashResolution(stage, task , subtask)
        println(s"Resolve Crash $crash \n $stage" +
          s" $task $subtask")
        TaskExecutionManager.fixUnresolvedCrashingRecord(crash, stage, task, subtask ,srnum, 1)
      case "trace" =>
        val stage: Int = Option(request.getParameter("stage")).getOrElse("").toInt
        val task: Int = Option(request.getParameter("task")).getOrElse("").toInt
        val subtask: Int = Option(request.getParameter("subtask")).getOrElse("").toInt
        val h_code: Int = Option(request.getParameter("linid")).getOrElse("").toInt
        println("Invoking Lineage Query")
        TaskExecutionManager.startLineageQuery(h_code , stage, task, subtask)
      case "skip" =>
       // val crash: String = Option(request.getParameter("record")).getOrElse("")
        val stage: Int = Option(request.getParameter("stage")).getOrElse("").toInt
        val task: Int = Option(request.getParameter("task")).getOrElse("").toInt
        val subtask: Int = Option(request.getParameter("subtask")).getOrElse("").toInt
        val srnum: Int = Option(request.getParameter("srnum")).getOrElse("").toInt

        //   TaskExecutionManager.simulateCrashResolution(stage, task , subtask)
        println("Skipping Record")
        TaskExecutionManager.fixUnresolvedCrashingRecord("", stage, task, subtask , srnum, 0)
      case "test" =>
      // NOTE: If we need to run something simple, we can use this command.
      case "set_guard" => {
        val regex = Option(request.getParameter("code")).getOrElse("")
        val rddid = Option(request.getParameter("rddid")).getOrElse("").toInt
        TaskExecutionManager.setExpression(regex, rddid)
      }
      case _ => {
        // do nothing
        DebugHelper.log("ERROR", "handleDebuggerCommand", "Invalid Command")
      }
    }
  }
}


