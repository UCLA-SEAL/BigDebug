package org.apache.spark.lineage.ui

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by ali on 8/10/17.
 */

class BSListenerBusImpl(conf: SparkConf) extends BigSiftUIListenerBus {

  val ui_listener = new BigSiftUIListenerImpl(conf, this)
  addListener(ui_listener)

  override def addListener(listener: BigSiftUIListener) {
    bsuiListeners += listener
  }
}

trait BigSiftUIListenerBus extends Logging {

  // SparkListeners attached to this event bus
  protected val bsuiListeners = new ArrayBuffer[BigSiftUIListener]
    with mutable.SynchronizedBuffer[BigSiftUIListener]

  protected val list_fli = new ArrayBuffer[FaultLocalizationInfo]
    with mutable.SynchronizedBuffer[FaultLocalizationInfo]

   var lastFaultInfo: Option[FaultLocalizationInfo] = None
   var initialSize: Option[Long] = None
   var initialJobTime: Option[Long] = None
   var totalLocalizationTime: Option[Long] = None
   var initialOutput: Option[String] = None
   var waitObjectForBigSIft : Object = new Object;

  def addListener(listener: BigSiftUIListener) {
    bsuiListeners += listener
  }

  def notifyBigSiftWait(): Unit ={
    waitObjectForBigSIft.synchronized {
      waitObjectForBigSIft.notifyAll();

    }
  }
  def waitForUICommand(): Unit ={
    waitObjectForBigSIft.synchronized {
      waitObjectForBigSIft.wait()
    }
  }
  /**
   * Post an event to all attached listeners.
   *
   */
  def postToAll(fli: FaultLocalizationInfo): Unit = {
    lastFaultInfo = Some(fli)
    list_fli += fli
    bsuiListeners.foreach { listener =>
      try {
        listener.report(fli)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }


  def postOutput(out: String): Unit = {
    initialOutput = Some(out)
    bsuiListeners.foreach { listener =>
      try {
        listener.postOutput(out)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }


  def postInitialJobTime(time:Long): Unit = {
    initialJobTime = Some(time)
    bsuiListeners.foreach { listener =>
      try {
        listener.postInitialJobTime(time)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }


  def postInitialSize(size:Long): Unit = {
    initialSize = Some(size)
    bsuiListeners.foreach { listener =>
      try {
        listener.postInitialSize(size)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }


  def postFinalLocalizationTime(time: Long): Unit = {
    totalLocalizationTime = Some(time)
    bsuiListeners.foreach { listener =>
      try {
        listener.postFinalLocalizationTime(time)
      } catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }
  }

  def getFinalDebuggingTime(): Option[String] ={
    if(totalLocalizationTime.isDefined) {
      val sec = totalLocalizationTime.get / 1000000l;
      Some(s"""{ "key": ${BigSiftWebUI.DEBUGTIME} , "data" : $sec }""")
    } else{
      None
    }
    }

  def getInitialJobTime(): Option[String] ={
    if(initialJobTime.isDefined) {
      val sec = initialJobTime.get / 1000000l;
      Some(s"""{ "key": ${BigSiftWebUI.JOBTIME} , "data" : $sec }""")
    } else{
      None
    }
  }


  def getOutput(): Option[String] ={
    if(initialOutput.isDefined) {
      Some(s"""{ "key": ${BigSiftWebUI.OUTPUT} , "data" : "${initialOutput.get}" }""")
    } else{
      None
    }
  }

  def getInitialsize(): Option[String] ={
    if(initialSize.isDefined) {
      val sec = initialSize.get;
      Some(s"""{ "key": ${BigSiftWebUI.SIZE} , "data" : $sec }""")
    } else{
      None
    }
  }

  def getFaultLocalizationJSONData(): String = {
    var msg = ""
    if(list_fli.size!=0){
      msg = getFaultLocalizationIterator().map{s =>
        var sample = ""
        if(s.topRecords.size != 0)
          sample = s.topRecords.toIterator.map(  s => s""" "$s" """).reduce( (v1,v2) => v1 + "," + v2)
        else
          sample = ""
        s"""{ "time": ${s.time/1000000l} , "size":${s.size} , "toprecords" : [${sample}] }"""
      }
        .reduce((a,b) => a + "," +b)
    } else{
      msg = "{}"
    }
    s"""{ "key": ${BigSiftWebUI.UIDATA} , "data" : [$msg]}"""
  }

  def getFaultLocalizationCSVData(): String = {
    if(list_fli.size!=0){
      "time;size;toprecords\n" +  getFaultLocalizationIterator().map(s => s.time + ";" + s.size + ";" ).reduce((a, b) => a + "\n" + b)
    } else {
      "time;size;toprecords"
    }
  }

  def getFaultLocalizationIterator(): Iterator[FaultLocalizationInfo] = {
    list_fli.toIterator
  }
}
