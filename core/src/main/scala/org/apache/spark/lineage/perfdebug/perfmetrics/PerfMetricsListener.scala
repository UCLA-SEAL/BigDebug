package org.apache.spark.lineage.perfdebug.perfmetrics

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.lineage.perfdebug.ignite.conf.IgniteManager
import org.apache.spark.scheduler._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/** Largely based off of StatsReportListener and JobProgressListener. This class collects task
 * metrics and then saves them for each job (within stages). This is still a testing listener
 * that WILL have memory leaks if run too long.
 *
 * This was imported from a secondary git repo (perf-debug-app) to include tracking of aggregate
 * metrics captured via instrumentation of the serializers/deserializers.
 */
class PerfMetricsListener(val initAppId: Option[String] = None) extends SparkListener {
  
  type TaskId = Long
  
  // App:
  @volatile var startTime = -1L
  @volatile var endTime = -1L
  @volatile var appId: String = initAppId.orNull
  
  
  // Jobs:
  val jobIdToStageIds = new mutable.HashMap[JobId, Seq[StageId]]
  val stageIdToInfo = new mutable.HashMap[StageId, StageInfo] // ultimately not used
  val stageIdToTaskIds = new mutable.HashMap[StageId, ListBuffer[TaskId]]
  val taskIdToTaskInfoAndMetrics = new mutable.HashMap[TaskId, (TaskInfo, TaskMetrics)]
  //Stages:
  
  override def onApplicationStart(appStarted: SparkListenerApplicationStart) {
    startTime = appStarted.time
    appId = appStarted.appId.getOrElse("undefined")
  }
  
  override def onApplicationEnd(appEnded: SparkListenerApplicationEnd) {
    endTime = appEnded.time
  }
  
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobIdToStageIds(jobStart.jobId) = jobStart.stageIds
  }
  
  def finalize(jobEnd: SparkListenerJobEnd): Unit = {
    // TODO: store a mapping of app -> job -> stage -> tasks. basically the equivalent of the RDD
    //  DAG, but for physical execution.
    val jobId = jobEnd.jobId
    
    /*println("JOB TO STAGE")
    jobIdToStageIds.foreach(println)
    println(s"Using $jobId...")
    println("Stage to Tasks")
    stageIdToTaskIds.foreach(println)
    println(s"Task ID to metrics (${taskIdToTaskInfoAndMetrics.size})")
    taskIdToTaskInfoAndMetrics.foreach(println)*/
    
    for(stageId <- jobIdToStageIds.remove(jobId).get) {
      // Not every stage will have completed tasks necessarily?
      if(!stageIdToTaskIds.contains(stageId))
      // TODO figure out why this happens.
        println(s"\t Warning?: Stage #$stageId did not have finished " + s"tasks")

      for(taskId <- stageIdToTaskIds.remove(stageId).map(_.sorted).getOrElse(Seq.empty)) {
        val (taskInfo, taskMetrics) = taskIdToTaskInfoAndMetrics.remove(taskId).get
        val partitionId = taskInfo.partitionId
        
        val stats: PerfMetricsStats = getMetrics(taskMetrics)
  
        PerfMetricsStorage.getInstance().savePerfMetrics(appId, jobId, stageId, partitionId, stats)
      }
    }
  }
  
  private def getMetrics(taskMetrics: TaskMetrics): PerfMetricsStats = {
    val shuffleReadMetrics = taskMetrics.shuffleReadMetrics
    val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
    val runtime = taskMetrics.executorRunTime // includes the CPU time
    val inputReadRecords = taskMetrics.inputMetrics.recordsRead
    val outputWrittenRecords = taskMetrics.outputMetrics.recordsWritten
    val shuffleWrittenBytes = shuffleWriteMetrics.bytesWritten
    val shuffleWriteRecords = shuffleWriteMetrics.recordsWritten
    val shuffleReadBytes = shuffleReadMetrics.remoteBytesRead
    val shuffleReadRecords = shuffleReadMetrics.recordsRead
    val shuffleReadFetchTime = shuffleReadMetrics.fetchWaitTime
    val resultSizeBytes = taskMetrics.resultSize
    val taskDeserializationTime = taskMetrics.executorDeserializeTime
    val taskResultSerializationTime = taskMetrics.resultSerializationTime
  
    /**
     * Java SerDe time metrics
     * Note: based on discussion/email thread between Jason and Khanh (6/1/18), our
     * instrumentation tracks:
     * 1. The writeObject and readObject instrumentations measure both IO + ser/deser
     * (JavaSeralizer.scala)
     * 2. The serialize/deserialize() instrumentation measures only ser/de time
     * (JavaSerializer.scala)
     * 3. TimeTrackingInputStream (and output) measure only IO
     * We note that ser/de time is present across two sets of APIs, (read|write)Object/(de)serialize
     * Thus, the total ser/de time is at least the sum of those. However, the
     * writeObject/readObject methods also encompass the TimeTracking streams (* - see note at
     * end), which is why they include both IO and ser/de time. To compute only the ser/de time
     * of these methods, we subtract the IO measured. Our final equation is then:
     *
     * IO time = #3
     * ser/de time = #1 + #2 - #3 (with #1 - #3 representing the ser/de of (write|read)Object)
     * combined = #1 + #2 (??? unsure)
     *
     * *note: TimeTrackingInputStream is definitely used in JavaSerializer. I haven't verified
     * TimeTrackingOutputStream's usage yet.
     */
    val shuffleWriteTime = shuffleWriteMetrics.writeTime
    val shuffleReadTime = shuffleReadMetrics.readTime
    // no longer being used
    //    val shuffleDataDeserializationTime = shuffleReadMetrics.dataDeserializationTime
    //    val shuffleDataSerializationTime = shuffleWriteMetrics.dataSerializationTime
    val shuffleWriteIOTime = shuffleWriteMetrics.writeIOTime
    val shuffleReadIOTime = shuffleReadMetrics.readIOTime
    val shuffleWriteSerializationTime = shuffleWriteTime - shuffleWriteIOTime
    val shuffleReadDeserializationTime = shuffleReadTime - shuffleReadIOTime
    
    val gcTime = taskMetrics.jvmGCTime
    
    /*if(false) {
          println(s"Task #${taskId}:)")
          println(s"\tExecutor runtime: ${runtime} ms")
          println(s"\tInput Records: ${inputRead}")
          println(s"\tOutput Records: ${outputWritten} (0 if nothing " +
            s"written to disk or PairLRDDFunctions does not support)")
        }*/
    /** Using named arguments to avoid potential issues with reordering */
    PerfMetricsStats(
      runtime=runtime,
      inputReadRecords=inputReadRecords,
      outputWrittenRecords=outputWrittenRecords,
      shuffleWrittenBytes=shuffleWrittenBytes,
      shuffleWriteRecords=shuffleWriteRecords,
      shuffleReadBytes=shuffleReadBytes,
      shuffleReadRecords=shuffleReadRecords,
      shuffleReadFetchTime=shuffleReadFetchTime,
      resultSizeBytes=resultSizeBytes,
      taskDeserializationTime=taskDeserializationTime,
      taskResultSerializationTime=taskResultSerializationTime,
      // Java SerDe times
      shuffleWriteTime=shuffleWriteTime,
      shuffleWriteIOTime=shuffleWriteIOTime,
      shuffleWriteSerializationTime=shuffleWriteSerializationTime,
      shuffleReadTime=shuffleReadTime,
      shuffleReadIOTime=shuffleReadIOTime,
      shuffleReadDeserializationTime=shuffleReadDeserializationTime,
      gcTime=gcTime)
  }
  
  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    finalize(jobEnd)
  }
  
  // override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit =
  
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit =
  {
    val stage = stageCompleted.stageInfo
    // Success!
    if(stage.failureReason.isEmpty) {
      stageIdToInfo(stage.stageId) = stage
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val taskInfo = taskEnd.taskInfo
    val taskId = taskInfo.taskId
    val taskMetrics = taskEnd.taskMetrics
    
    val taskIds = stageIdToTaskIds.getOrElseUpdate(taskEnd.stageId, ListBuffer[TaskId]())
    taskIds += taskId
    taskIdToTaskInfoAndMetrics(taskId) = (taskInfo, taskMetrics)
  }
  
}
