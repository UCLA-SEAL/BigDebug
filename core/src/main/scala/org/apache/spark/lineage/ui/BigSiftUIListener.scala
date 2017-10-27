package org.apache.spark.lineage.ui

import org.apache.spark.internal.Logging
import org.apache.spark.{SecurityManager, SparkConf}

/**
 * Created by ali on 8/10/17.
 */
/**
 * Base Listener Interface for BigSift to report fault localization info at runtime
 **/
trait BigSiftUIListener {

  def report(fli: FaultLocalizationInfo): Unit = {}
  def postInitialJobTime(time:Long) : Unit = {}
  def postInitialSize(size:Long): Unit = {}
  def postFinalLocalizationTime(time: Long): Unit = {}
  def postOutput(out:String): Unit = {}
}

class BigSiftUIListenerImpl(conf: SparkConf, bigSiftUIListenerBus: BigSiftUIListenerBus) extends BigSiftUIListener with Logging {

  var bigsiftSocket: Option[SocketRunner] = _
  val sectMgr = new SecurityManager(conf)
  logInfo("Starting BigSift UI  at " + BigSiftWebUI.BigSiftWeb_UI_PORT)
  val webUi = new BigSiftWebUI(sectMgr, conf, BigSiftWebUI.BigSiftWeb_UI_PORT, bigSiftUIListenerBus)
  assert(webUi.bigsiftWebSocket != null, "Web Socket is null")
  setSocket(webUi.bigsiftWebSocket)
  logInfo("Setting web socket in Listener Bus at port " + bigsiftSocket.get.port)
  webUi.bind()

  override def report(fli: FaultLocalizationInfo): Unit = {
    if(bigsiftSocket.isDefined)
      bigsiftSocket.get.socket.updateBigSiftUI(bigSiftUIListenerBus.getFaultLocalizationJSONData())
    else
      logInfo(s"""Socket for BigSift is not Defined""")
  }

  override def postInitialJobTime(time:Long) : Unit = {
    bigsiftSocket.get.socket.updateBigSiftUI(bigSiftUIListenerBus.getInitialJobTime().get)
  }
  override  def postInitialSize(size:Long): Unit = {

    bigsiftSocket.get.socket.updateBigSiftUI(bigSiftUIListenerBus.getInitialsize().get)
  }
  override def postFinalLocalizationTime(time: Long): Unit = {
    bigsiftSocket.get.socket.updateBigSiftUI(bigSiftUIListenerBus.getFinalDebuggingTime.get)

  }
  override def postOutput(out:String): Unit = {
    bigsiftSocket.get.socket.updateBigSiftUI(bigSiftUIListenerBus.getOutput.get)
  }

  def setSocket(socket: SocketRunner): Unit = {
    bigsiftSocket = Some(socket)
  }
}

case class FaultLocalizationInfo(time: Long, size: Long, runs: Int , topRecords : Array[String])

