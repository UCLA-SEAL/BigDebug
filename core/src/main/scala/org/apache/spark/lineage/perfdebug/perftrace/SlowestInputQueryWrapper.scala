package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.lineageV2.{HadoopLineageWrapper, LineageCache, LineageCacheDependencies, LineageWrapper}
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, TapHadoopLRDDValue}
import org.apache.spark.rdd.RDD
import LineageCache._
/**
 * Implementation of [[org.apache.spark.lineage.perfdebug.lineageV2.HadoopLineageWrapper]] targets
 * support for queries for slowest input records. Notably, the functionality is distinct enough
 * that it is not an instance of [[PerfLineageWrapper]]. However, it does provide special methods
 * for querying slowest inputs. This class specifically assumes use of hadoop input.
 *
 * This implementation is primarily used with [[SlowestInputsCalculator]].
 */
class SlowestInputQueryWrapper(override val lineageDependencies: LineageCacheDependencies,
                               // the list of base input values (primarily used as IDs) and
                               // their corresponding impact scores (used for ranking)
                               val valuesWithScores: RDD[(TapHadoopLRDDValue, Long)],
                               ) extends HadoopLineageWrapper(lineageDependencies,
                                                              // need to cast to CacheValue for
                                                              // LineageCache API
                                                              valuesWithScores.keys.map(v =>
                                                                                           (v.key,
                                                                                            v.asInstanceOf[CacheValue])
                                                              )) {
  def inputOrdering: Ordering[(TapHadoopLRDDValue, Long)] = Ordering.by(_._2)
  def takeSlowestInputs(n: Int): SlowestInputQueryWrapper = {
    val slowestValuesWithScores: Array[(TapHadoopLRDDValue, Long)] =
      valuesWithScores.top(n)(inputOrdering)
    val resultRDD: RDD[(TapHadoopLRDDValue, Long)] = valuesWithScores.context.parallelize(slowestValuesWithScores)
    new SlowestInputQueryWrapper(lineageDependencies, resultRDD) // this is mostly to convert to
    // a hadoop wrapper
  }
  
}


