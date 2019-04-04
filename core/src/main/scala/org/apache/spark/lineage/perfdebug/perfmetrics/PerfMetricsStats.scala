package org.apache.spark.lineage.perfdebug.perfmetrics

import scala.collection.immutable.ListMap

case class PerfMetricsStats(runtime: Long, inputReadRecords: Long,
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
                            shuffleReadDeserializationTime: Long,
                            /** end Java SerDe metrics */
                            gcTime: Long) {
  // TODO: Try serializing/deserializing from a standardized format (eg CSV).
  
  /*def toValueTuple(clazz: Class[_]): Any = {
    clazz match {
      case FULL_VALUE_CLASS =>
        PerfMetricsStats.unapply(this).get
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
  }*/
  
  /** Deprecated subset of fields.
   *  case class JavaSerDeValue(
                               shuffleWriteTime: Long,
                               shuffleWriteIOTime: Long,
                               shuffleWriteSerializationTime: Long,
                               shuffleReadTime: Long,
                               shuffleReadIOTime: Long,
                               shuffleReadDeserializationTime: Long)
   */
  
  // https://stackoverflow.com/a/1227643/6890456
  def asMap: Map[String, Any] = this.getClass.getDeclaredFields.foldLeft(ListMap[String, Any]()){
    (map, field) =>
      field.setAccessible(true)
      map + (field.getName -> field.get(this))
  }
  
  def asMapStr: String = {
    this.asMap.toString().replace("Map", this.getClass.getSimpleName)
  }
}
