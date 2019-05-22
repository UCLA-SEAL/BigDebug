package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.Latency
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.TapHadoopLRDDValue

import scala.collection.mutable

/** Mutable tuple classes for use with
 * [[org.apache.spark.lineage.perfdebug.perftrace.SlowestInputsCalculator]].
 * This class is an extension of [[SingleRmLatencyTuple]] designed to allow tracking of multiple
 * bottlenecks per slowest input.
 */
case class MultipleRmLatencyTuple private(var latencies: mutable.Seq[Latency],
                                          var bottlenecks: mutable.Seq[TapHadoopLRDDValue],
                                          // flag to indicate whether or not removal of `slowest`
                                          // results in deletion of the corresponding output record.
                                          // While true, rmLatency should be 0.,
                                          var rmAllBottlenecksLatency: Latency,
                                          var isDestructiveRemoval: Boolean) extends RmLatencyTuple {
  override def latency: Latency = latencies.head
  override def rmLatency: Latency = latencies.lift(1).getOrElse(0)
  override def slowest: TapHadoopLRDDValue = bottlenecks.head
  
  override def toString: String = s"MultipleRmLatencyTuple($latency->$rmLatency ms, inp=${slowest
    .key}${if (isDestructiveRemoval) "*" else ""}) TODO"
}

object MultipleRmLatencyTuple {
  def apply(value: TapHadoopLRDDValue): MultipleRmLatencyTuple = {
    MultipleRmLatencyTuple(mutable.Seq(value.latency),
                           mutable.Seq(value),
                           0,
                           isDestructiveRemoval = true)
  }
  
  // These two functions are fixed and final for this implementation.
  // merge the 'max' record from earlier with the stage-latency of the current stage.
  // n = # of bottlenecks to store for each record - it's not actually used here but retained for
  // consistency.
  def accFn(n: Int): (MultipleRmLatencyTuple, Latency) => MultipleRmLatencyTuple =
    (tuple, stgLatency) => {
      // mutate in place
      tuple.latencies.transform(_ + stgLatency)
      if(!tuple.isDestructiveRemoval) {
        tuple.rmAllBottlenecksLatency += stgLatency
      }
      // return the original
      tuple
    }
  
  // n = # of bottlnecks to store for each record.
  def aggFn(n: Int): (MultipleRmLatencyTuple, MultipleRmLatencyTuple) => MultipleRmLatencyTuple =
    (first, second) => {
      // TODO optimize this one later
      val isDestr = first.isDestructiveRemoval || second.isDestructiveRemoval ||
        Set(first.bottlenecks.union(second.bottlenecks)).size <= n
      
      
      
      // OLD STUFF BELOW
      // explicit way of essentially sorting and assigning variables, but I want to avoid
      // intentionally creating new objects in memory.
      val slowerTuple = if(first.latency >= second.latency) first else second
      val fasterTuple = if(slowerTuple == first) second else first
      
      val resultTuple = slowerTuple // chosen to minimize reassignments, which are left as comments
      /*resultTuple.rmLatency = Math.max(slowerTuple.rmLatency,
                                       // if removing slowest would affect the fasterTuple, use
                                       // rmLat. Otherwise, use the original latency.
                                       if(slowerTuple.slowest == fasterTuple.slowest) {
                                         fasterTuple.rmLatency
                                       } else {
                                         fasterTuple.latency
                                       })*/
      
      // resultTuple.slowest = slowerTuple.slowest
      // resultTuple.latency = slowerTuple.latency
      
      // finally: since we're aggregating, it means more than one input goes into the corresponding
      // output. Then the removal of an input might not destroy the output record, and we set the
      // corresponding flag.
      resultTuple.isDestructiveRemoval = false
      
      resultTuple
    }
}


