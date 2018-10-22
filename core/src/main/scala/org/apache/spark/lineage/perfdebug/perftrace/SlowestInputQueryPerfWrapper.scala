package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.lineageV2.LineageCache._
import org.apache.spark.lineage.perfdebug.lineageV2.{HadoopLineageWrapper, LineageCache, LineageCacheDependencies}
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, PartitionWithRecId, TapHadoopLRDDValue}
import org.apache.spark.rdd.RDD

/**
 * Implementation of [[org.apache.spark.lineage.perfdebug.lineageV2.HadoopLineageWrapper]] targets
 * support for queries for slowest input records. Notably, the functionality is distinct enough
 * that it is not an instance of [[PerfLineageWrapper]]. However, it does provide special methods
 * for querying slowest inputs. This class specifically assumes use of hadoop input.
 *
 * This implementation is primarily used with [[SlowestInputsCalculator]].
 */
case class SlowestInputQueryPerfWrapper(private val lineageDependencies: LineageCacheDependencies,
                                        outputIdsWithLatencyTuples: RDD[(PartitionWithRecId,
                                          SingleRmLatencyTuple)],
                                        baseRDD: RDD[(PartitionWithRecId, CacheValue)]
                               ) extends IdOnlyPerfLineageWrapper(lineageDependencies,
                                                                  outputIdsWithLatencyTuples
                                                                    .mapValues(_.latency),
                                                                  baseRDD) {
  def inputOrdering: Ordering[(TapHadoopLRDDValue, Long)] = Ordering.by(_._2)
  
  private lazy val slowestInputsHadoopWrapper: SlowestInputsHadoopLineageWrapper = {
    // We've precomputed the results for slowest inputs by retaining the input IDs along the way.
    // These IDs are guaranteed to be non-trivial (ie they will have an impact on output latency if
    // removed). However, we need to do one final aggregation - for each input ID, we want to
    // retain the smallest impact it has on any output (min(latency - rmLatency)). This is to
    // reflect the worst-case scenario where every output is computed in parallel; in such a
    // situation, removing the record can only improve total job performance by the minimum of
    // the tuple calculations.
    val inputsToLatencyImpact: RDD[(TapHadoopLRDDValue, Long)] =
      outputIdsWithLatencyTuples.values.map(tuple =>
                                              (tuple.slowest, tuple.latency - tuple.rmLatency))
      // TODO partitioner? (note that we expect top(n) to be called later which inherently
      // executes a job though.
      .reduceByKey(Math.min)
    // In addition to finding that, HadoopLineageWrapper requires the lineage cache dependencies,
    // so use traceBackAll to find that.
    val sourceDependencies = traceBackAllSources().map(_.lineageDependencies).toSet // dedup
    if(sourceDependencies.size > 1) {
      throw new IllegalStateException("Slowest input queries currently do not support multiple" +
                                        " input files")
    }
    new SlowestInputsHadoopLineageWrapper(sourceDependencies.head, inputsToLatencyImpact)
  }
  
  def takeSlowestInputs(n: Int): SlowestInputsHadoopLineageWrapper = {
    slowestInputsHadoopWrapper.top(n)
  }
  
}


