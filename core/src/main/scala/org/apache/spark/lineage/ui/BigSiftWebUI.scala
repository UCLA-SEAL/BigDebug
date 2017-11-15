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

package org.apache.spark.lineage.ui

import org.apache.spark.internal.Logging
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.{JettyUtils, SparkUI, UIUtils, WebUI}
import org.apache.spark.{SecurityManager, SparkConf}

import scala.xml.Node

/**
 *
 * Web UI server for the standalone executor by bigdebug.
 */
private[spark]
class BigSiftWebUI(
                    sectMgr: SecurityManager,
                    conf: SparkConf,
                    requestedPort: Int , bslistbus: BigSiftUIListenerBus)
  extends WebUI(sectMgr, sectMgr.getSSLOptions("ui"), BigSiftWebUI.BigSiftWeb_UI_PORT, conf,"",  name = "ExecutorUI")
//ebUI(securityManager, securityManager.getSSLOptions("ui"), SparkUI.getUIPort(conf),
//conf, basePath, "SparkUI")
  with Logging {

 // val timeout = AkkaUtils.askTimeout(conf)
  var bigsiftWebSocket: SocketRunner = null

  def getSparkConf(): SparkConf ={
    conf
  }
  def getExecutorWebSocketPort(): Int = {
    bigsiftWebSocket.getCurrentPort()
  }

  initialize()

  /** Initialize all components of the server. */
  def initialize() {
    /** *
      *
      * BD starts
      * */
    bigsiftWebSocket = new SocketRunner(BigSiftWebUI.BigSiftWeb_WEBSOCKET_PORT)
    bigsiftWebSocket.run()
    val profilePage = new BSUIPage(this , bslistbus)
    attachPage(profilePage)
    attachHandler(createStaticHandler(BigSiftWebUI.STATIC_RESOURCE_BASE, "/static"))
    attachHandler(JettyUtils.createRedirectHandler("/do", "/", profilePage.handleDebuggerCommand))
  }
}

private[spark] object BigSiftWebUI {
  val STATIC_RESOURCE_BASE = SparkUI.STATIC_RESOURCE_DIR
  val BigSiftWeb_UI_PORT = 8989
  val BigSiftWeb_WEBSOCKET_PORT = 41222
  val UIDATA = 1
  val JOBTIME = 2
  val SIZE = 3
  val DEBUGTIME = 4
  val OUTPUT = 5
  val OUTPUTVIZ = 6


  /** Returns a page with the spark css/js and a simple format. Used for scheduler UI. */
  def basicSparkPage(content: => Seq[Node], title: String , onLoad:String = "" , additionalHeaders: Seq[Node] = Seq.empty ): Seq[Node] = {
    <html>
      <head>
        {UIUtils.commonHeaderNodes}
        {additionalHeaders}
        <title>{title}</title>
      </head>
      <body onload={onLoad} >
        <div class="container-fluid">
          <div class="row-fluid">
            <div class="span12">
              <h3 style="vertical-align: middle; display: inline-block;">
                <a style="text-decoration: none" href={UIUtils.prependBaseUri("/")}>
                  <img src={UIUtils.prependBaseUri("/static/spark-logo-77x50px-hd.png")} />
                  <span class="version"
                        style="margin-right: 15px;">{org.apache.spark.SPARK_VERSION}</span>
                </a>
                {title}
              </h3>
            </div>
          </div>
          {content}
        </div>
      </body>
    </html>
  }

}
