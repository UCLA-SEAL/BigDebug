package org.apache.spark.lineage.ui

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by ali on 8/10/17.
 */

class BSListenerBusImpl(conf: SparkConf , sc: Option[SparkContext] = None) extends BigSiftUIListenerBus {

  sparkContext = sc
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

  var jobId: Int = -1
  var sparkContext: Option[SparkContext] = None
  var lastFaultInfo: Option[FaultLocalizationInfo] = None
  var initialSize: Option[Long] = None
  var initialJobTime: Option[Long] = None
  var totalLocalizationTime: Option[Long] = None
  var initialOutput: Option[String] = None
  var waitObjectForBigSIft : Object = new Object;
  var initialOutputViz: Option[mutable.ArraySeq[Any]] = None
  var dagInfo :  Option[List[(Int, Long, Long)]] = None

  /**
    * User Defined Test Predicated Handler
    */
  val compiler = new BDCodeFixCompiler(None)
  var testType :Int = -1;
  var test : Option[Any=> Boolean] = None;

  def setTestType(s:Int): Unit ={
    testType = s;
  }

  def compilePredicate(code:String): Unit ={
    val pc: BDCodeFix = compiler.eval[BDCodeFix](code)
    setFunction(pc)
  }

  def setFunction(p: BDCodeFix): Unit ={
    test = Some(p.test)
  }

  /***/

  def setDAGInfoMap(s: mutable.Map[Int, (RDD[Any] , Long)] ): Unit ={
    var map = List[(Int, Long, Long)]()
    var adj = 0
    for((k,v) <- s){
      map =   (k ,v._1.count() , v._2) :: map
    }
    var temp = map.sortBy(_._1)
    map = List[(Int, Long, Long)]()
    for((k, v1, v2) <- temp) {
      if (v2 > v1) {
        adj = adj-1
      }else {
        map = (k + adj, v1, v2) :: map
      }
    }
    dagInfo = Some(map.sortBy(_._1))
  }


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


  def postOutput(out: String , arr: mutable.ArraySeq[Any] = null): Unit = {
    initialOutput = Some(out)
    if(arr != null ){
      initialOutputViz = Some(arr);
    }
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


  def getOutputForViz(withKey: Boolean = true ): Option[String] ={
    var outputStr = ""
    if(initialOutputViz.isDefined) {
      initialOutputViz.get.head match {
        case  a : Tuple2[Any, Any] =>
              a.asInstanceOf[Tuple2[Any, Any]]._2 match {
                case _ :Int =>
                  var typed_arr =  initialOutputViz.get.asInstanceOf[mutable.ArraySeq[Tuple2[Any, Int]]]
                    outputStr = typed_arr.map( s => s""" {"key": "${s._1.toString.replace("\"", "\\\"")}" , "value" : ${s._2} } """  ).reduce(_+","+_)
                case _ :Float =>
                  var typed_arr =  initialOutputViz.get.asInstanceOf[mutable.ArraySeq[Tuple2[Any, Float]]]
                  outputStr = typed_arr.map( s => s""" {"key": "${s._1.toString.replace("\"", "\\\"")}" , "value" : ${s._2} } """  ).reduce(_+","+_)
                case _ : String =>
                  var typed_arr =  initialOutputViz.get.asInstanceOf[mutable.ArraySeq[Tuple2[Any, String]]]
                  outputStr = typed_arr.map( s => s""" {"key": "${s._1.toString.replace("\"", "\\\"")}" , "value" : "${s._2}" } """  ).reduce(_+","+_)

                case _ => logInfo("Output type not supported")
              }
        case _  => logInfo("Output type not supported")
      }
      if(withKey){
        Some(s""" {"key" :  ${BigSiftWebUI.OUTPUTVIZ}, "data" : [ ${outputStr} ]  }""")
      }else{
        Some(s"""[ ${outputStr} ]""")
      }
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
          sample = s.topRecords.toIterator.map(  s => s""" "${s.replace("\"", "\\\"")}" """).reduce( (v1,v2) => v1 + "," + v2)
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
