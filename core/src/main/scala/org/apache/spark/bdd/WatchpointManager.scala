package org.apache.spark.bdd

/** BDD START **/

import java.io.{File, FileOutputStream, IOException}

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{SendACKCode, SendWatchpointDataToDriver}

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.tools.nsc.interpreter.AbstractFileClassLoader
import scala.tools.nsc.io._

object WatchpointManager extends Logging {
  private var sparkContext: SparkContext = null
  private val rdds = new HashMap[(Int, Int), RDD[_]]

  def setSparkContext(sparkContext: SparkContext): Unit = {
    this.sparkContext = sparkContext
  }

  def setRDD(stageId: Int, partitionId: Int, rdd: RDD[_]): Unit = {
    rdds.synchronized {
      //	DebugHelper.log("WARNING", "WatchpointSupport", s"setRDD: $stageId, $partitionId")
      rdds((stageId, partitionId)) = rdd
    }
  }

  private val watchPoint_Data = HashMap[(Int, Int)/*TaskID and RDD ID*/, List[CapturedRecord]]().withDefaultValue(Nil)


  private var predicateClass = new BDWatchpointPredicate {
   def check(key: String, value: String): Boolean = {
      return true
    }
  }

  def getExpression: BDWatchpointPredicate = {
    predicateClass
  }

  def setExpression(list: List[Int], class_name: String): Unit = {
    val myExecutorId = BDExecutorManager.GetExecutorId
      BDExecutorManager.sendMessage(SendACKCode(1, "Expression Received : " + class_name + " of size : " + list.length, myExecutorId))
    writePredicateClass(class_name, list)
  }

  def setExpression(list: List[Int], class_name: String, rddID: Int): Unit = {
    println("Expression Received")
    val myExecutorId = BDExecutorManager.GetExecutorId
      BDExecutorManager.sendMessage(SendACKCode(1, "Expression Received : " + class_name + " of size : " + list.length, myExecutorId))
    writePredicateClass(class_name, list, rddID)
  }

  def setCodeFix(codeStore: mutable.HashMap[Int, (String , List[Int])]): Unit = {
    println("Expression Received")
    val myExecutorId = BDExecutorManager.GetExecutorId
    for(k <- codeStore.keySet){
      var va = codeStore(k)
      writePredicateClass(va._1 , va._2 , k)
    }
      BDExecutorManager.sendMessage(SendACKCode(1, "Expression Received : " , codeStore.keySet.size.toString))
  }

  def writePredicateClass(class_name: String, list: List[Int]): Unit = {
    var out = None: Option[FileOutputStream]
    var file = new File("/tmp")
    if (!file.exists()) {
      file.mkdir()
    }
    try {

      out = Some(new FileOutputStream("/tmp/" + class_name + ".class"))
      for (value <- list) {
        out.get.write(value)
      }

    } catch {
      case e: IOException => logInfo(e.getCause.getMessage)
    } finally {
      if (out.isDefined) out.get.close
    }

    try {

      val target = AbstractFile.getDirectory(file)
      val classLoader = new AbstractFileClassLoader(target, this.getClass.getClassLoader)
      val cls = classLoader.loadClass(class_name)
      predicateClass = cls.getConstructor().newInstance().asInstanceOf[BDWatchpointPredicate]
    } catch {
      case ex: Exception => logInfo(ex.getCause.getMessage)
    }
  }

  /**
   *
   * Rewriting the filter function
   *
   **/
  private val filtersMap = HashMap[Int, (Class[_], Int)]()
  def getCodeFixClass(rddID: Int): Class[_] = {
    if (filtersMap.contains(rddID)) {
      val (cls, old_flip) = filtersMap(rddID)
     return   cls
      }
    else
      null
  }

  def getFilterClass(rddID: Int, flip: Int): Class[_] = {
    if (filtersMap.contains(rddID)) {
      val (cls, old_flip) = filtersMap(rddID)
      if (old_flip == flip) {
        null
      } else {
        cls
      }
    }
    else
      null
  }

  def writePredicateClass(class_name: String, list: List[Int], rddID: Int): Unit = {
    var out = None: Option[FileOutputStream]
    var file = new File("/tmp/worker")
    if (!file.exists()) {
      file.mkdir()
    }
    try {
      out = Some(new FileOutputStream("/tmp/worker/" + class_name + ".class"))
      for (value <- list) {
        out.get.write(value)
      }
    } catch {
      case e: IOException => logInfo(e.getCause.getMessage)
    } finally {
      if (out.isDefined) out.get.close
    }

    try {

      val target = AbstractFile.getDirectory(file)
      val classLoader = new AbstractFileClassLoader(target, this.getClass.getClassLoader)
      val cls = classLoader.loadClass(class_name)
      if (filtersMap.contains(rddID)) {
        val flip = filtersMap(rddID)._2
        filtersMap(rddID) = (cls, flip + 1)
      }
      else
        filtersMap(rddID) = (cls, 0)
      logInfo("Predicate class settled")

    } catch {
      case ex: Exception => logInfo(ex.getCause.getMessage)
    }
  }

  def captureWatchpointRecord(t: String, taskID: Int, rddID: Int): Unit = {
    watchPoint_Data.synchronized {
      watchPoint_Data((taskID, rddID)) ::= CapturedRecord(t,taskID,rddID)
    }
  }

  def setTaskDone(taskID: Int, rddID: Int, context:TaskContext): Unit = {
    val myExecutorId = BDExecutorManager.GetExecutorId
    watchPoint_Data.synchronized {
      if (context.bdconfig.WATCHPOINT_DATA_SEND_TO_DRIVER
           && watchPoint_Data((taskID, rddID)).length > 0) {
        BDExecutorManager.sendMessage(SendWatchpointDataToDriver(myExecutorId, watchPoint_Data((taskID, rddID)), rddID))
      }
      watchPoint_Data((taskID, rddID)) = List[CapturedRecord]()
    }
  }

}

case class CapturedRecord(record: String, partitionID: Int, rddID:Int)

/** BDD END **/