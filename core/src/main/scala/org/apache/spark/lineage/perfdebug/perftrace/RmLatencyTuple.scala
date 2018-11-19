package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.TapHadoopLRDDValue

trait RmLatencyTuple {
  def latency: Long
  def rmLatency: Long
  def slowest: TapHadoopLRDDValue
  def isDestructiveRemoval: Boolean
}
