package org.apache.spark.debugging

import akka.actor.ActorRef
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{ReleaseLock, NotifyCrashCulprit}
import org.apache.spark.{SparkContext, TaskContextImpl, TaskContext}

import scala.collection.mutable.{HashMap}
;

object LineageHandler {


  var lineageContext: LineageContext = null
  var topRDD: RDD[_] = null
  var disableTopRDDset = false;
  var currenJobID = -1
  var sparkContext: SparkContext = null
  var currentCrash: UnresolvedCrashRecords = null
  var waitObject : Object = new Object();
  private val executorActor = new HashMap[String, ActorRef]


  def setSparkContext(sc: SparkContext): Unit = {
    sparkContext = sc
  }

  def setLineageContext(lc: LineageContext): Unit = {
    lineageContext = lc
  }

  def registerExecutor(id: String, ex: ActorRef): Unit = {
    executorActor(id) = ex
  }



  def setCrash(record: Any, subtaskID: Int, exception: Exception, context: TaskContext , lineageID : Long): Any = {
    val stageID = context.stageId()
    val taskID = context.partitionId()
    val currentID = context match {
      case _: TaskContextImpl =>
        val a = context.asInstanceOf[TaskContextImpl].currentInputId
          a
      case _ =>
        -1
    }
    val driver = ExecutorManager.GetDriver
    if (sparkContext == null) {
      driver ! NotifyCrashCulprit(record.toString, ExecutorManager.GetExecutorId, stageID, taskID, subtaskID, exception, true, currentID)
    }
    else{
      enrollCrash(record.toString ,stageID, taskID, subtaskID, exception, true,"" ,  currentID)
    }


    waitObject.synchronized {
      waitObject.wait()
      DebugHelper.log("INFO", "LineageHandler", s"Wait Relseased on $record")
    }

    }
  def setCrash(record: Any, subtaskID: Int, exception: Exception, lineageID : Int): Any = {

    val driver = ExecutorManager.GetDriver
    if (sparkContext == null) {
      driver ! NotifyCrashCulprit(record.toString, ExecutorManager.GetExecutorId, -1, -1, subtaskID, exception, true, lineageID)
      waitObject.synchronized {
        waitObject.wait()
        DebugHelper.log("INFO", "LineageHandler", s"Wait Relseased on $record")
      }
    }
    else{
       val requestThread = new Thread(new Runnable {
        def run(): Unit = {
          enrollCrash(record.toString ,-1, -1, subtaskID, exception, true,"" ,  lineageID)
        }
      })
      requestThread.start();

      requestThread.synchronized {
        requestThread.wait()
        DebugHelper.log("INFO", "LineageHandler", s"Wait Relseased on $record")
     }

    }

  }

    def setTopRDD(rdd: RDD[_], id: Int): Unit = {
      if (!disableTopRDDset) {
        topRDD = rdd
        currenJobID = id
      }
      // println("Job ID:" + id)
    }

    def getRDDFromId(rddID: Int): RDD[_] = {
      var rdd = topRDD
      if (rdd.id == rddID) {
        return topRDD
      }
      var break = true
      while (!rdd.dependencies.isEmpty && break) {
        rdd = rdd.dependencies.head.rdd
        if (rdd.id == rddID)
          break = false

      }
      if (break == false) {
        return rdd
      } else {
        null
      }
    }

    def enrollCrash(record: String, stageID: Int, taskID: Int, subtaskID: Int, exception: Exception, waiting: Boolean, sender: String, currentID: Int): Unit = {
      DebugHelper.log("INFO", "TaskExecutorManager", s"Enroll Crash ( $stageID , $taskID , $subtaskID ) $record  $currentID Error: $exception")
      currentCrash = new UnresolvedCrashRecords(record, stageID, taskID, subtaskID, currentID)
      startLineageQuery();
      releaseLock(sender);
    }

    def getLineageofCrashingRecord(rddID: Int, lineageID: Int): Long = {
      if (lineageContext == null) {
        return -1L
      }
      val rdd = getRDDFromId(rddID)

      def getTapRDD(rdd: RDD[_]): TapLRDD[_] = {

        rdd.dependencies(0).rdd match {
          case tap: TapLRDD[_] => tap
          case other => getTapRDD(other)
        }
      }
      val tap = if(rdd.isInstanceOf[TapLRDD[_]])  rdd else getTapRDD(rdd)

      disableTopRDDset = true
      tap match {
        case h: TapHadoopLRDD[_, _] =>
          getLineage[Long, String](tap.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
            .map(r => (r._1.get(), r._2.toString)), lineageID.asInstanceOf[Int])
            .cache().collect().foreach(println)
        case t: TapLRDD[_] =>
          lineageContext.setCulprit()
          // set last Lineage

          lineageContext.setCurrentLineagePosition(Some(t))
          lineageContext.getLineage(t)

          val tmp = tap.asInstanceOf[Lineage[(Any, Array[Int] ) ]]
//               get.map(r => (r._1._2, r._2.asInstanceOf[Array[Int]]))
          val lineage = new LineageRDD(getLineage(tmp, lineageID.asInstanceOf[Int]).flatMap(r => r._2.map(b => (r._1, (Dummy, b)))))//flatMap(r => r._2.map(b => (r._1.asInstanceOf[Tuple2[Any, Any]]._2, b))))
          val a = lineage.goBackAll()
               a.collect().foreach(println)
          println("showing lineage now")
          a.show(true)
      }
      disableTopRDDset = false;
      0L
    }

    def getLineage[T, V](prev: Lineage[(T, V)], next: T) = {
       prev.filter { current =>
       //  true
        current._1 match {
          case t : Tuple2[_, _] =>
            t._2 == next
          case _ =>
            current._2 == next
        }
      }
    }

    def startLineageQuery(): Unit = {
      var lin_id: Int = 0
      var rddId = -1
      // if (currentCrash.linID.hashCode() == h_code) {
      lin_id = currentCrash.linID
      rddId = currentCrash.rddID
      //    }
      /*else {

      }
        for (record <- unresolvedCrashes(stage, task, subtask)) {
          if (record.linID.hashCode() == h_code) {
            lin_id = record.linID
            rddId = record.rddID
          }
        }
      }*/
      getLineageofCrashingRecord(rddId, lin_id)

    }

  def releaseLocalLock(): Unit = {
    waitObject.synchronized {
      waitObject.notify()
    }
  }
    def releaseLock(exeID : String): Unit ={
   // val actor = executorActor(exeID)
    if(executorActor.contains(exeID)){
      executorActor(exeID) ! ReleaseLock()
    }else{
      waitObject.synchronized {
        waitObject.notify()
      }
    }
  }
  }



case class UnresolvedCrashRecords(record: String, stageID: Int, taskID: Int, rddID: Int, linID:Int)
