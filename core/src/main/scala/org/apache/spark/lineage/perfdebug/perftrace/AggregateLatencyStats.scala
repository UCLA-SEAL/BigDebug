package org.apache.spark.lineage.perfdebug.perftrace

/** Class used to help approximate shuffle-based latency at a partition level. The metrics
 *  involved represent metrics per partition.
 */
case class AggregateLatencyStats(numInputs: Long, numOutputs: Long, latency: Long)
