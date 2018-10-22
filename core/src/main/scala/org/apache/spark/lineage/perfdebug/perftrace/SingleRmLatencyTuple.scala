package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.TapHadoopLRDDValue

/** Mutable tuple classes for use with
 * [[org.apache.spark.lineage.perfdebug.perftrace.SlowestInputsCalculator]]
 */
case class SingleRmLatencyTuple private(var latency: Long,
                                        var rmLatency: Long,
                                        var slowest: TapHadoopLRDDValue,
                                        // flag to indicate whether or not removal of `slowest`
                                        // results in deletion of the corresponding output record.
                                        // While true, rmLatency should be 0.
                                        var isDestructiveRemoval: Boolean) extends RmLatencyTupleTrait {
  override def toString: String = s"SingleRmLatencyTuple($latency->$rmLatency ms, inp=${slowest
    .key}${if (isDestructiveRemoval) "*" else ""})"
}

object SingleRmLatencyTuple {
  def apply(value: TapHadoopLRDDValue): SingleRmLatencyTuple = {
    SingleRmLatencyTuple(value.latency, 0L, value, isDestructiveRemoval = true)
  }
  
  // These two functions are fixed and final for this implementation.
  // merge the 'max' record from earlier with the stage-latency of the current stage.
  val accFn: (SingleRmLatencyTuple, Long) => SingleRmLatencyTuple = (tuple, stgLatency) => {
    // reuse tuple and update values.
    tuple.latency += stgLatency
    if (!tuple.isDestructiveRemoval) {
      // Removing the slowest record does not completely delete this record. As such, when
      // crossing stages we will also need to update the hypothetical removal latency.
      tuple.rmLatency += stgLatency
      // We don't change the flag yet, because there's a chance that this particular input record
      // is the only/unique record corresponding to a key in a later shuffle, in which case an
      // output record could still be destroyed by removal of this input record even after shuffles.
    }
    // no need to update 'slowest' element, as that doesn't change across accumulation.
    tuple
  }
  
  val aggFn: (SingleRmLatencyTuple, SingleRmLatencyTuple) => SingleRmLatencyTuple = (first, second) => {
    // explicit way of essentially sorting and assigning variables, but I want to avoid
    // intentionally creating new objects in memory.
    val slowerTuple = if(first.latency >= second.latency) first else second
    val fasterTuple = if(slowerTuple == first) second else first
    
    val resultTuple = slowerTuple // chosen to minimize reassignments, which are left as comments
    resultTuple.rmLatency = Math.max(slowerTuple.rmLatency,
                                     // if removing slowest would affect the fasterTuple, use
                                     // rmLat. Otherwise, use the original latency.
                                     if(slowerTuple.slowest == fasterTuple.slowest) {
                                       fasterTuple.rmLatency
                                     } else {
                                       fasterTuple.latency
                                     })
    // resultTuple.slowest = slowerTuple.slowest
    // resultTuple.latency = slowerTuple.latency
    
    // finally: since we're aggregating, it means more than one input goes into the corresponding
    // output. Then the removal of an input might not destroy the output record, and we set the
    // corresponding flag.
    resultTuple.isDestructiveRemoval = false
    
    resultTuple
  }
}
