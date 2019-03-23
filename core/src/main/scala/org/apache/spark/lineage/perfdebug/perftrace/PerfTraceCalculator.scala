package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.{Latency, Partitioner}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Entry point for performance tracing. See the implementations for algorithm-specific
 * details: [[PerfTraceCalculatorV1]], [[PerfTraceCalculatorV2]]
 */
trait PerfTraceCalculator {
  // TODO trait consider changing this to abstract class instead.
  /** Entry point for public use */
  def calculate(): PerfLineageWrapper
  
  // Note: These could be generalized (generics) but are only used for this specific use case
  // right now
  // TODO: convert count into an int.
  protected case class AggResultWithCount(var aggResult: Latency, var count: Long) {
    def mergeWith(other: AggResultWithCount, mergeFn: (Latency, Latency) => Latency): AggResultWithCount = {
      this.aggResult = mergeFn(this.aggResult, other.aggResult)
      this.count += other.count
      this
    }
  }
  
  // Note: These could be generalized (generics) but are only used for this specific use case
  // right now. This class wrapper takes a keyed latency rdd and supports agg + count operations
  protected implicit class AggCountRDD[K](rdd: RDD[(K, Latency)])(implicit keyTag: ClassTag[K])
    extends Serializable { // TODO serializable is undesirable, but somewhere along the line this
    // class is being serialized (likely due to complex field references and spark's closure
    // cleaner checking). There are some hints about using a 'shim' function to clean things up
    // in the following post, but for the time being it's much simpler to just mark this implicit
    // class as serializable.
    // http://erikerlandson.github.io/blog/2015/03/31/hygienic-closures-for-scala-function-serialization/
    
    //implicit val ct = scala.reflect.classTag[AggResultWithCount]
    /** Mirrors reduceByKey including default partitioner */
    // no longer in use!
    /*
      def reduceByKeyWithCount(fn: (Long, Long) => Long): RDD[(K, AggResultWithCount)] = {
      val (createCombinerAggCount, mergeValueAggCount, mergeCombinerAggCount) =
        createCombineFnsForReduce(fn)
      rdd.combineByKeyWithClassTag(createCombinerAggCount,
                                   mergeValueAggCount,
                                   mergeCombinerAggCount)
    }*/

    def reduceByKeyWithCount(part: Partitioner,
                             fn: (Latency, Latency) => Latency): RDD[(K, AggResultWithCount)] = {
      val (createCombinerAggCount, mergeValueAggCount, mergeCombinerAggCount) =
        createCombineFnsForReduce(fn)
      rdd.combineByKeyWithClassTag(createCombinerAggCount,
                                   mergeValueAggCount,
                                   mergeCombinerAggCount,
                                   part)
    }
    
    private def createCombineFnsForReduce(fn: (Latency, Latency) => Latency) = {
      // create V=> C - technically indep of fn
      val createCombinerAggCount = (v: Latency) => AggResultWithCount(v, 1)
      // mergeV(C, V) => C
      val mergeValueAggCount = (c: AggResultWithCount, v: Latency) => {
        c.aggResult = fn(c.aggResult, v)
        c.count += 1
        c
      }
      
      // mergeC(C, C) => C
      val mergeCombinerAggCount = (c1: AggResultWithCount, c2: AggResultWithCount) => {
        c1.mergeWith(c2, fn)
      }
      
      (createCombinerAggCount, mergeValueAggCount, mergeCombinerAggCount)
    }
    
  }
}
