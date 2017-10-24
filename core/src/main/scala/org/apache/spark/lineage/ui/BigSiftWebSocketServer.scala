package org.apache.spark.lineage.ui

/**
 * Created by ali on 1/29/16.
 */

import java.net.InetSocketAddress

import org.apache.spark.internal.Logging
import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer

import scala.collection.mutable


class BigSiftWebSocketServer(port: Int, runner: SocketRunner) extends WebSocketServer(new InetSocketAddress(port)) with Logging{

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

  def updateBigSiftUI(msg: String): Unit = {
    var set: Set[WebSocket] = Set()
    val con = connections().iterator()
    while (con.hasNext) {
      set += con.next()
    }
    logInfo(s""""Sending fault localization info : $msg""")
    for (s <- set) {
          s.send(msg)
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

class SocketRunner(p: Int) {
  var socket : BigSiftWebSocketServer = null
  var port = p

  def run() {
    socket = new BigSiftWebSocketServer(port, this)
    socket.start()
  }

  def getCurrentPort(): Int = {
    port
  }

  def onError(): Unit = {
    socket.stop()
    socket = new BigSiftWebSocketServer(port + 1, this)
    port = port + 1
    socket.start()

  }

}

