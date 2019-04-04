package org.apache.spark.lineage.perfdebug.perfmetrics

trait PerfMetricsStorage {
  // TODO: Consider grouping app+job+stage id into a single parameter object/class (eg tuple).
  /* TODO: figure out rdd ID to stage ID mapping...
   it's not simple, you'll need to parse the DAG and identify shuffle deps...
   doesn't the Spark WebUI have something like this already? how is that accomplished?
   Also, how do I plan to 'attribute' stage metrics to individual RDDs, if possible?? Maybe I
   should upload first and then figure out later??
  */
  /** Retrieves all partition stats for the given stage, as a map with key = partition and value =
   * stats for that partition.
   */
  def getPerfMetricsForStage(appId: AppId,
                             jobId: JobId,
                             stageId: StageId): Map[PartitionId, PerfMetricsStats]
  
  /** Saves partition stats for the given stage. */
  def savePerfMetrics(appId: AppId,
                      jobId: JobId,
                      stageId: StageId,
                      partitionId: PartitionId,
                      data: PerfMetricsStats): Unit
}

object PerfMetricsStorage {
  // TODO make this configurable via conf in the future. Also consider integrating with SparkEnv
  private var _instance: Option[PerfMetricsStorage] = None
  def getInstance(): PerfMetricsStorage = {
    if(_instance.isEmpty) {
      // TODO remove default instantiation eventually
      _instance = Some(new IgnitePerfMetricsStorage())
    }
    _instance.getOrElse(
      throw new IllegalStateException("No AggregateStatsRepo instance has been set. Did you " +
                                        "mean to call AggregateStatsRepo.setInstance(...)?")
    )
  }
  
  def setInstance(instance: PerfMetricsStorage) =
    _instance = Option(instance)
  
  val KEY_CLASS = classOf[StageId]
  val VALUE_CLASS = classOf[PerfMetricsStats]
  
  def COARSE_GRAINED_SCHEMA_STR(keyClass: Class[_]=KEY_CLASS,
                                valueClass: Class[_]= VALUE_CLASS): String = {
    s"(${keyClass.getName.capitalize} -> ${getFieldNames(valueClass)})}"
  }
  
  private def getFieldNames(clazz: Class[_]): String = {
    val result: Array[String] = clazz.getDeclaredFields.map(_.getName.capitalize)
    s"(${result.mkString(",")})"
  }
}

