package org.apache.spark.ui.debugger

/**
 * Created by ali on 1/29/16.
 */

import java.net.InetSocketAddress

import org.apache.spark.bdd.{BDDMetricsSupport, BigDebugConfiguration}
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer

import scala.collection.mutable


class DebuggerWebSocketServer(port: Int, runner: SocketRunner, conf:BigDebugConfiguration) extends WebSocketServer(new InetSocketAddress(port)) {

  var websocketsToRDDs = mutable.HashMap[Int, Int]()

  def onWebsocketOpen(): Unit = {
    println("Socket Opened + " + port)
  }

  override def onOpen(webSocket: WebSocket, clientHandshake: ClientHandshake): Unit = {
   // println("Connection Opened")
    webSocket.send("Established")
  }

  override def onError(webSocket: WebSocket, e: Exception): Unit = {
    println("Error : " + e.getStackTraceString)
    runner.onError();
  }

  override def onMessage(webSocket: WebSocket, s: String): Unit = {
   // println("Message received")
    if (s.startsWith("Init ")) {
      websocketsToRDDs(webSocket.hashCode()) = s.split(" ")(1).toInt
    }

  }

  def updateRecordProfileUI(rdd: Int): Unit = {
    var set: Set[WebSocket] = Set()
    val con = connections().iterator()
    while (con.hasNext) {
      set += con.next()
    }
    for (s <- set) {
      if (websocketsToRDDs.getOrElse(s.hashCode(), -1) == rdd) {
        s.send(BDDMetricsSupport.getUiProfileData(rdd))
      }
    }
  }

  def updateWatchPointRDDs(rdd: Int): Unit = {
    println("Updating Watchpoint")
    var set: Set[WebSocket] = Set()
    val con = connections().iterator()
    while (con.hasNext) {
      set += con.next()
    }
    for (s <- set) {
      if (websocketsToRDDs.getOrElse(s.hashCode(), -1) == rdd) {
       s.send(WatchpointPage.renderContent(rdd).toString)
      }
    }
  }

  /**
   * This function can also be used to update the watchpoint
   * @param rdd
   */
  def updateCrashedRDDs(rdd: Int): Unit = {
    //println("Updating Crash")
    var set: Set[WebSocket] = Set()
    val con = connections().iterator()
    while (con.hasNext) {
      set += con.next()
    }
    for (s <- set) {
      if (websocketsToRDDs.getOrElse(s.hashCode(), -1) == rdd) {
        s.send(RDDPage.renderContent(rdd, conf).toString)
      }
    }
  }
  def sendErrorLineNumber(rdd: Int , str:String): Unit = {
    println("Updating Crash : " + str)
    var set: Set[WebSocket] = Set()
    val con = connections().iterator()
    while (con.hasNext) {
      set += con.next()
    }
    for (s <- set) {
      if (websocketsToRDDs.getOrElse(s.hashCode(), -1) == rdd) {
        s.send(str)
      }
    }
  }
  def updateDebuggerTaskProfiling(str:String): Unit = {
    //println("Updating Crash")
    var set: Set[WebSocket] = Set()
    val con = connections().iterator()
    while (con.hasNext) {
      set += con.next()
    }
    for (s <- set) {
      if (websocketsToRDDs.getOrElse(s.hashCode(), -1) == -1) {
        s.send(str)
      }
    }

  }

  def sendToAll(text: String) {
    // println(s"""Sending message to UI port: $port   and text : \n $text \n\n""")
    var set: Set[WebSocket] = Set()
    val con = connections().iterator()
    while (con.hasNext) {
      set += con.next()
    }
    for (s <- set) {
      s.send(text)
    }
  }

  def updateMetaData(): Unit = {

  }

  override def onClose(webSocket: WebSocket, i: Int, s: String, b: Boolean): Unit = {}
}

class SocketRunner(port: Int, conf:BigDebugConfiguration) {
  var s: DebuggerWebSocketServer = null
  var p = port

  def run() {
    s = new DebuggerWebSocketServer(p, this, conf)
    s.start()
  }

  def getCurrentPort(): Int = {
    p
  }

  def onError(): Unit = {
    s.stop()
    s = new DebuggerWebSocketServer(p + 1, this, conf)
    p = p + 1
    s.start()

  }

}

