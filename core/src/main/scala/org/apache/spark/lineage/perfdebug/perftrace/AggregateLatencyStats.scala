package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.Latency

/** Class used to help approximate shuffle-based latency at a partition level. The metrics
 *  involved represent metrics per partition.
 *  // TODO: store latency as long here, since it might sum up to more than int.
 */
case class AggregateLatencyStats(numInputs: Long, numOutputs: Long, latency: Latency)
