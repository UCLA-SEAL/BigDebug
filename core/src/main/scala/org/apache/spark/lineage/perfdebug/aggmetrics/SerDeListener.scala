package org.apache.spark.lineage.perfdebug.aggmetrics

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.{Ignite, IgniteCache}
import org.apache.spark.executor.TaskMetrics
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
class SerDeListener(val ignite: Ignite, val initAppId: Option[String] = None) extends
  SparkListener {
  
  import SerDeListener._
  //private val taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()
  type JobId = Int
  type JobGroupId = String
  type StageId = Int
  type StageAttemptId = Int
  // type PoolName = String
  // type ExecutorId = String
  type TaskId = Long
  
  // App:
  @volatile var startTime = -1L
  @volatile var endTime = -1L
  @volatile var appId: String = initAppId.orNull
  
  
  // Jobs:
  val jobIdToStageIds = new mutable.HashMap[JobId, Seq[StageId]]
  val stageIdToInfo = new mutable.HashMap[StageId, StageInfo]
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
    val jobId = jobEnd.jobId
    for(stageId <- jobIdToStageIds(jobId)) {
      val cacheName = coarseGrainedCacheName(appId, jobId, stageId)
      val igniteCache = createIgniteCache(cacheName)
      // Not every stage will have completed tasks necessarily?
      if(!stageIdToTaskIds.contains(stageId))
      // TODO figure out why this happens.
        println(s"\t Warning?: Stage #$stageId did not have finished " + s"tasks")
      for(taskId <- stageIdToTaskIds.getOrElse(stageId, Seq.empty).sorted) {
        val key: CoarseGrainedKey = CoarseGrainedKey(taskId)
        val value: CoarseGrainedValue = getMetrics(key)
        
        if(igniteCache.containsKey(key)) {
          println(s"Replacing key $key:\n"
                    + s"\tOld: ${igniteCache.get(key)}\n"
                    + s"\tNew: ${value}")
        }
        
        igniteCache.put(key, value)
      }
    }
  }
  private def getMetrics(key: CoarseGrainedKey): CoarseGrainedValue = {
    // TaskInfo not used right now.
    val (taskInfo, taskMetrics) = taskIdToTaskInfoAndMetrics(key.taskId)
    val shuffleReadMetrics = taskMetrics.shuffleReadMetrics
    val shuffleWriteMetrics = taskMetrics.shuffleWriteMetrics
    // taskInfo.duration not sure how this differs from executorRunTime?
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
    
    
    
    /*if(false) {
          println(s"Task #${taskId}:)")
          println(s"\tExecutor runtime: ${runtime} ms")
          println(s"\tInput Records: ${inputRead}")
          println(s"\tOutput Records: ${outputWritten} (0 if nothing " +
            s"written to disk or PairLRDDFunctions does not support)")
        }*/
    /** Using named arguments to avoid potential issues with reordering */
    CoarseGrainedValue(
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
      shuffleReadDeserializationTime=shuffleReadDeserializationTime)
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
  
  /** Temp clone of LineageManager's method to easily support schema changes during development. */
  private def createIgniteCache(cacheName: String, numPartitionsPerCache: Int = 2)
  : IgniteCache[CoarseGrainedKey, CoarseGrainedValue] = {
    val cacheConf = new CacheConfiguration[CoarseGrainedKey, CoarseGrainedValue](cacheName)
      .setAffinity(new RendezvousAffinityFunction(false, numPartitionsPerCache))
    
    val cache: IgniteCache[CoarseGrainedKey, CoarseGrainedValue] = ignite.getOrCreateCache(cacheConf)
    cache
  }
  
}

object SerDeListener {
  def coarseGrainedCacheName(appId: Any, jobId: Any, stageId: Any): String =
    s"$appId-job-$jobId-stage-$stageId"
  
  val COARSE_GRAINED_CACHE_NAME_PATTERN: Regex = "job-\\d+-stage-\\d+$".r // job-stage
  
  // TODO move these to a more appropriate class
  case class CoarseGrainedKey(taskId: Long) extends Ordered[CoarseGrainedKey] {
    override def compare(that: CoarseGrainedKey): Int = this.taskId.compareTo(that.taskId)
  }
  
  case class CoarseGrainedValue(runtime: Long, inputReadRecords: Long,
                                outputWrittenRecords: Long, shuffleWrittenBytes: Long,
                                shuffleWriteRecords: Long, shuffleReadBytes: Long,
                                shuffleReadRecords: Long, shuffleReadFetchTime: Long,
                                resultSizeBytes: Long, taskDeserializationTime: Long,
                                taskResultSerializationTime: Long,
                                /** Java SerDe metrics */
                                shuffleWriteTime: Long,
                                shuffleWriteIOTime: Long,
                                shuffleWriteSerializationTime: Long,
                                shuffleReadTime: Long,
                                shuffleReadIOTime: Long,
                                shuffleReadDeserializationTime: Long) {
    
    def toValueTuple(clazz: Class[_]): Any = {
      clazz match {
        case COARSE_GRAINED_VALUE_CLASS =>
          CoarseGrainedValue.unapply(this).get
        case JAVA_SERDE_VALUE_CLASS =>
          // hacky, but there to ensure easy changes in the future
          JavaSerDeValue.unapply(
            JavaSerDeValue(
              shuffleWriteTime=shuffleWriteTime,
              shuffleWriteIOTime=shuffleWriteIOTime,
              shuffleWriteSerializationTime=shuffleWriteSerializationTime,
              shuffleReadTime=shuffleReadTime,
              shuffleReadIOTime=shuffleReadIOTime,
              shuffleReadDeserializationTime=shuffleReadDeserializationTime)
          ).get
        case _ =>
          throw new IllegalArgumentException(s"Unsupported class for value conversion: $clazz")
      }
    }
  }
  
  case class JavaSerDeValue(/** Java SerDe metrics */
                            shuffleWriteTime: Long,
                            shuffleWriteIOTime: Long,
                            shuffleWriteSerializationTime: Long,
                            shuffleReadTime: Long,
                            shuffleReadIOTime: Long,
                            shuffleReadDeserializationTime: Long)
  
  private val COARSE_GRAINED_KEY_CLASS = classOf[CoarseGrainedKey]
  private val COARSE_GRAINED_VALUE_CLASS = classOf[CoarseGrainedValue]
  private val JAVA_SERDE_VALUE_CLASS = classOf[JavaSerDeValue]
  def COARSE_GRAINED_SCHEMA_STR(keyClass: Class[_]=COARSE_GRAINED_KEY_CLASS,
                                valueClass: Class[_]=COARSE_GRAINED_VALUE_CLASS): String = {
    s"(${getFieldNames(keyClass)}, ${getFieldNames(valueClass)})}"
  }
  
  private def getFieldNames(clazz: Class[_]): String = {
    val result: Array[String] = clazz.getDeclaredFields.map(_.getName.capitalize)
    s"(${result.mkString(",")})"
  }
  
}
