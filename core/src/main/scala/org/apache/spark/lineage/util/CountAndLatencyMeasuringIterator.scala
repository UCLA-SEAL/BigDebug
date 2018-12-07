package org.apache.spark.lineage.util

import org.apache.spark.lineage.rdd.Lineage

class CountAndLatencyMeasuringIterator[T](iter: Iterator[T]) extends Iterator[T] {
  private var _latency = 0L
  private var _count = 0L
  
  def latency = _latency
  def count = _count
  
  override def hasNext: Boolean = measureLatency { iter.hasNext }
  
  override def next(): T = measureLatency {
    _count += 1
    iter.next()
  }
  
  private def measureLatency[R](block: => R): R =
  // TODO shuffle flag (instrumentation toggle)
    Lineage.measureTimeWithCallback(block,
                                    _latency += _)
}
