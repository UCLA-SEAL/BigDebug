package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.Latency
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.TapHadoopLRDDValue

trait RmLatencyTuple {
  def latency: Latency
  def rmLatency: Latency
  def slowest: TapHadoopLRDDValue
  def isDestructiveRemoval: Boolean
}
