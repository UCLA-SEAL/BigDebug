package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.Latency
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
  def inputOrdering: Ordering[(TapHadoopLRDDValue, Latency)] = Ordering.by(_._2)
  
  /**
   * A Wrapper representing each input record and the difference between A) the maximum latency
   * across all output records and B) the maximum rmLatency across all output records.
   *
   * The impact here is strictly with respect to the outputs of the selected record. In other
   * words, application performance may not necessarily (and generally will not) improve as much
   * as predicted by the impact score of a removed record.
   *
   * Algorithm approach: inputs -> max(outputs.latency) - max(outputs.rmLatency)
   */
  private lazy val slowestInputsHadoopWrapper: SlowestInputsHadoopLineageWrapper = {
    // We've precomputed the results for slowest inputs by retaining the input IDs along the way.
    // These IDs are guaranteed to be non-trivial (ie they will have an impact on output latency if
    // removed). However, we need to do one final aggregation - we want to compute the difference
    // between maximum latency with and without the input record in question (max(latency) - max
    // (rmLatency).This is to reflect the 'worst-case' scenario where every output is computed in parallel;
    // in such a situation, removing the record can only improve total job performance by
    // difference between maximum latencies.
    // it's not really a worst case scenario though?
    val inputsToLatencyImpact: RDD[(TapHadoopLRDDValue, Latency)] = {
      /* outputIdsWithLatencyTuples.values.map(tuple =>
                                              (tuple.slowest, tuple.latency - tuple.rmLatency))
      // TODO partitioner? (note that we expect top(n) to be called later which inherently
      // executes a job though.
      .reduceByKey(Math.min) */
      outputIdsWithLatencyTuples.values.map(
        tuple => {
//          println(tuple)
          (tuple.slowest, (tuple.latency, tuple.rmLatency))
        })
        .reduceByKey({ case ((lat1, rmLat1), (lat2, rmLat2)) =>
          (Math.max(lat1, lat2), Math.max(rmLat1, rmLat2))
        })
        .mapValues({case (maxLat, maxRmLat) => maxLat - maxRmLat})
    }
    
    
    
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
  
  /**
   * Very similar to [[takeSlowestInputs()]], but restricted to only one record. The calculation
   * of individual input record impacts is the similar, but no difference is taken between the
   * maximum latency and rmLatency. Furthermore, the final returned impact value is modified in
   * an attempt to better model application performance.
   *
   * Previously (original version) we used the individual record's impact to predict application
   * performance improvement. This implicitly relies on the assumption that the application
   * performance is still dominated by the output records associated with the now-removed input
   * record. For example, if input A produces outputs O1,O2,O3, then we estimate the performance
   * improvement of removing A purely based on performance of O1,O2,O3 and not any other output
   * records.
   *
   * To remedy this shortcoming, this approach takes the top 2 records. The initial estimate of
   * application performance (pre-removal) remains the largest latency. However, the
   * post-removal latency estimate is the max of the removed record's rmLatency and the
   * 2nd-slowest record's latency.
   *
   * Note: this implementation was tested for evaluation and produced unexpected results that
   * still need to be evaluated. Some impact values were 0 (WordCount, HistogramRating). The
   * applications themselves did see some performance improvements, so this estimate isn't quite
   * correct for reasons unknown (could be approach, implementation, or some other unknown factor)
   * */
  def takeSingleSlowestInputBeta(): SlowestInputsHadoopLineageWrapper = {
    // V2 - when restricting to max, sort by max latency and then test with removal...
    val inputsToLatencyImpact: RDD[(TapHadoopLRDDValue, Latency)] = {
      /* outputIdsWithLatencyTuples.values.map(tuple =>
                                              (tuple.slowest, tuple.latency - tuple.rmLatency))
      // TODO partitioner? (note that we expect top(n) to be called later which inherently
      // executes a job though.
      .reduceByKey(Math.min) */
      val aggInputs: RDD[(TapHadoopLRDDValue, (Latency, Latency))] = outputIdsWithLatencyTuples.values
          .map(
            tuple => {
              //          println(tuple)
              (tuple.slowest, (tuple.latency, tuple.rmLatency))
          }).reduceByKey({ case ((lat1, rmLat1), (lat2, rmLat2)) =>
              (Math.max(lat1, lat2), Math.max(rmLat1, rmLat2))
            })
        
      
      
      val top2 = aggInputs.top(2)(Ordering.by(_._2._1))
      val slowest = top2.head._1 // Latency
      val impact = top2.head._2._1- Math.max(top2.head._2._2, top2.last._2._1) // Latency - max
      // (RmLat, second.Latency)
  
      println("V2 Debugging - agg by key, then take top and second-top")
      println(top2.head == top2.last)
      println(top2.head)
      println(top2.last)
      context.parallelize(Seq((slowest, impact)))
    }
  
  
  
    // In addition to finding that, HadoopLineageWrapper requires the lineage cache dependencies,
    // so use traceBackAll to find that.
    val sourceDependencies = traceBackAllSources().map(_.lineageDependencies).toSet // dedup
    if(sourceDependencies.size > 1) {
      throw new IllegalStateException("Slowest input queries currently do not support multiple" +
                                        " input files")
    }
    new SlowestInputsHadoopLineageWrapper(sourceDependencies.head, inputsToLatencyImpact).top(1)
  }
  
}


