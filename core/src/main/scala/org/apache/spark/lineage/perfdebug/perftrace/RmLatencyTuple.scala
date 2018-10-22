package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.TapHadoopLRDDValue

trait RmLatencyTuple {
  var latency: Long
  var rmLatency: Long
  var slowest: TapHadoopLRDDValue
  var isDestructiveRemoval: Boolean
}
