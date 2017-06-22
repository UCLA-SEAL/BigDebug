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

package org.apache.spark.executor.ui

import javax.servlet.http.HttpServletRequest

import org.apache.spark.executor.CoarseGrainedExecutorBackend
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.debugger.SocketRunner
import org.apache.spark.ui.{SparkUI, WebUI}

/**
 *
 * Web UI server for the standalone executor by bigdebug.
 */
private[spark]
class ExecutorWebUI(
    val executor: CoarseGrainedExecutorBackend,
    requestedPort: Int)
  extends WebUI(executor.sectMgr, requestedPort, executor.getConf(), name = "ExecutorUI")
  with Logging {

  def getExecutorWebSocketPort(): Int ={
    executorWebSocket.getCurrentPort()
  }

  var executorWebSocket: SocketRunner = null
  val timeout = AkkaUtils.askTimeout(executor.getConf())

  initialize()

  /** Initialize all components of the server. */
  def initialize() {
    /***
      *
      * BD starts
      * */
    executorWebSocket = new SocketRunner(ExecutorWebUI.EXECUTOR_WEBSOCKET_PORT ,executor.getBigDebugConfiguration() )
    executorWebSocket.run()
    val profilePage = new RecordProfilingPage(this)
    attachPage(profilePage)
    attachHandler(createStaticHandler(ExecutorWebUI.STATIC_RESOURCE_BASE, "/static"))
    attachHandler(createServletHandler("/profiledata",
      (request: HttpServletRequest) => profilePage.renderData(request), executor.sectMgr))


  }
}

private[spark] object ExecutorWebUI {
  val STATIC_RESOURCE_BASE = SparkUI.STATIC_RESOURCE_DIR
  val EXECUTOR_UI_PORT = 10101
  val EXECUTOR_WEBSOCKET_PORT = 20202
}
