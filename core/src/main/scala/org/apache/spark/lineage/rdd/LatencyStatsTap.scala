package org.apache.spark.lineage.rdd

import org.apache.spark.lineage.perfdebug.storage.AggregateLatencyStats

trait LatencyStatsTap[T] extends TapLRDD[T] {
  def getLatencyStats: AggregateLatencyStats
}

trait PreShuffleLatencyStatsTap[T] extends LatencyStatsTap[T] {
  @transient private var latencyStats: AggregateLatencyStats = _
  override def getLatencyStats: AggregateLatencyStats = latencyStats
  def setLatencyStats(stats: AggregateLatencyStats): Unit = latencyStats = stats
}


trait PostShuffleLatencyStatsTap[T] extends LatencyStatsTap[T] {
  // Warning: We could potentially be storing these stats multiple times per downstream
  // dependency even though it doesn't actually change. For simplicity/abstraction, we leave this
  // as is - it's already messy enough that the actual stats are stored in the ShuffledRDD.
  
  // Note: there's no setLatencyStats function here because the stats should be automatically set
  // from the parent (ShuffledLRDD).
  
}
