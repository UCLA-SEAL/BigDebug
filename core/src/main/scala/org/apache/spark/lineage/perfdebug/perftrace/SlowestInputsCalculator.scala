package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.{Latency, Partitioner}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.lineage.PerfDebugConf
import org.apache.spark.lineage.perfdebug.lineageV2.{LineageCacheDependencies, LineageWrapper}
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, EndOfStageCacheValue, PartitionWithRecId, TapHadoopLRDDValue}
import org.apache.spark.lineage.perfdebug.utils.TapUtils._
import org.apache.spark.lineage.perfdebug.utils.{PartitionWithRecIdPartitioner, PerfLineageUtils}
import org.apache.spark.lineage.rdd.{TapHadoopLRDD, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._

import scala.reflect.ClassTag


/**
 * New implementation of [[PerfTraceCalculator]], specifically designed to answer queries for the
 * slowest input records. Input records are ranked based on the impact of the removal of any one
 * single record.
 *
 * This implementation is an extension of [[PerfTraceCalculatorV2]]. It adds on two additional
 * 'columns':
 * 1. SlowestInput: The base input ID (eg hadoop ID) for the responsible input record whose
 * removal would result in a decreased latency.
 * 2. RmLat: The latency after removal of SlowestInput.
 *
 * Because of the nature of this computation, the acc and agg functions are restricted to sum and
 * max respectively.
 *
 * Fields are roughly calculated as follows:
 * 1. Latency: same as before, max(input latencies) + stage latency. This is initialized to 0 for
 * the input lineage RDD.
 * 2. Slowest: argmax-like operation on the max(input latencies) from above, retrieving the
 * corresponding Slowest parameter. This field is initialized to the outputID for the input
 * lineage RDD.
 * 3. RmLat: same as Latency, but substituting the max latency for its corresponding RmLat (from
 * the prior stage) and taking the max over the modified set of latencies. This is initialized to 0.
 *
 * @jteoh 10/16/2018
 */
case class SlowestInputsCalculator(@transient initWrapper: LineageWrapper,
                                   // flag that indicates whether or the output records in
                                   // initWrapper should be traced back to their inputs. This
                                   // adds additional stages via a lineage trace, but can greatly
                                   // reduce the workload in cases where the number of
                                   // output-contributing inputs is significantly smaller than
                                   // the number of total inputs. Disabling this reduces the
                                   // total number of spark joins involved, but may result in
                                   // unnecessary computation.
                                   // tl;dr: true = first compute backwards lineage trace before
                                   // computing forwards trace for latency calcs. May be more
                                   // expensive if trace takes a while, but much cheaper if
                                   // traced size << total input size
                                   traceInputScope: Boolean = true,
                                   printDebugging: Boolean = false,
                                   printLimit: Option[Int] = None) extends PerfTraceCalculator {
  type PartitionId = Int
  type InputId = PartitionWithRecId
  type OutputId = PartitionWithRecId
  type InputLatency = Latency
  type UnAccumulatedLatency = Latency // for OutputValue latencies that haven't been acc'ed yet.
  type OutputLatency = Latency
  type BaseInputId = TapHadoopLRDDValue
  type RmLatency = Latency // tuple after slowest base input has been removed
  type RecursiveSchema = (OutputId, SingleRmLatencyTuple)
  type RmLatencyTupleWithCount = AggregatedResultWithCount[SingleRmLatencyTuple]
  
  // These two functions are fixed and final for this implementation.
  // merge the 'max' record from earlier with the stage-latency of the current stage.
  val accFn = SingleRmLatencyTuple.accFn
  val aggFn = SingleRmLatencyTuple.aggFn
  
  /** Entry point for public use */
  def calculate(): SlowestInputQueryPerfWrapper = {
    val outputRmLatTuples: RDD[(OutputId, SingleRmLatencyTuple)] =
      perfTraceRecursiveHelper(initWrapper)
    initWrapper.asSlowestInputQueryWrapper(outputRmLatTuples)
  }
  
  
  
  
  /** The 'main' body of code appears after many of these methods (which reuse arguments) are
   * defined. The main computation is actually [[perfTraceRecursiveHelper]]
   */
  
  // ---------- START DEBUG PRINT HELPERS ----------
  def debugPrint(rdd: => RDD[_], msg: => String): Unit = {
    if(printDebugging)
      PerfLineageUtils.printRDDWithMessage(rdd,
                                           "[DEBUG PERFORMANCE TRACING] " + msg,
                                           limit = printLimit)
  }

  def debugPrintStr(msg: => String): Unit = {
    if(printDebugging) println(msg)
  }
  
  // ---------- END DEBUG PRINT HELPERS ----------
  
  // ---------- START AGG STATS REPO HELPERS ----------
  @transient
  private val aggStatsRepo = AggregateStatsStorage.getInstance()
  
  def getRDDAggStats(
                     latencyTapId: Int,
                     numPartitions: Int
                    ): Map[PartitionId, AggregateLatencyStats] =
    aggStatsRepo.getAllAggStats(initWrapper.lineageAppId, latencyTapId, numPartitions)
  // ---------- END AGG STATS REPO HELPERS ----------
  
  // ---------- START PERF TRACE HELPERS ----------
  def prevPerfRDDs(curr: LineageWrapper): Seq[RDD[RecursiveSchema]] = {
    // most of the time (except post cogroup), this is a single wrapper.
    val dependencies = curr.dependencies
    val prevWrappers = if(traceInputScope) {
      dependencies.indices.map(curr.traceBackwards)
    } else {
      dependencies.map(LineageWrapper.apply) // full/base lineage cache
    }
    val prevPerfWrappers = prevWrappers.map(perfTraceRecursiveHelper)
    prevPerfWrappers
  }
  
  /** For getting the first prevPerfRDD. Includes an assert to ensure no incorrect usage! */
  def prevPerfRDD(curr: LineageWrapper): RDD[RecursiveSchema] = {
    assert(curr.dependencies.length == 1, "Single PerfRdd can only be returned if there is only" +
      " one dependency. Otherwise, use the more general prevPerfRdds method.")
    prevPerfRDDs(curr).head
  }
  
  // ---------- END PERF TRACE HELPERS ----------
  
  
  /** Used within start-of-stage tracing, eg PostCoGroup, where there can be multiple
   *  input RDDs but no partial latency. This will also automatically incorporate the shuffle agg
   *  stats. */
  def joinInputRddsWithAggStats(aggDep: LineageCacheDependencies,
                                base: RDD[(OutputId, CacheValue)],
                                first: RDD[RecursiveSchema],
                                secondOption: Option[RDD[RecursiveSchema]] = None
                          ): RDD[RecursiveSchema] = {
    // assumption: each input latency RDD is uniquely keyed. for example, they come from the
    // prevPerfRDD set of methods. Additionally, there are no assumptions made on the
    // partitioning scheme for any input arguments. Finally, each record in base can have
    // multiple inputs - in the case when there are multiple input RDDs, the only guarantee is
    // that these record inputs come from at least one of the two input RDDs (not necessarily
    // both)
    // In practice, Titian does not support cogroup beyond 2 RDDs. This method is implemented
    // under such an assumption, but should be easily extendable if required.
    
    // We define a partitioner upfront to help boost performance (hopefully). This is used for
    // partitioning when the entire record (PartitionRecWithId + CacheValue) is the key.
    val partitioner = secondOption match {
      case Some(_) =>
        
        // We're going to do a 3-way join eventually after some reduceByKeys, so use a
        // consistent scheme to improve performance.
        // TODO jteoh the number of partitions might not be optimized here. Also, we need to
        // ensure we have enough partitions for each of the inputs against which we join. There's
        // technically no strict requirement (yet) that the output Partition ids match
        // appropriately, we're only using this for efficiency.
        val numPartitions = Partitioner.defaultPartitioner(base, first, secondOption.get).numPartitions
        //val resultPartitioner = new PRIdTuplePartitioner(numPartitions)
        val resultPartitioner = new PartitionWithRecIdPartitioner(numPartitions)
        resultPartitioner
      case None =>
        // In the future, we might still want to partition based on number in base, as base
        // tends to be the start of a stage (so the end-of-stage tap could have a 1-1 partition
        // mapping if we appropriately store the cache data in the same partitioning scheme)
        Partitioner.defaultPartitioner(base, first)
    }
    
    // The key here is inherently not unique, eg an output may come from multiple input
    // partitions. After joining to input latencies, we have to reduce by the output key.
    val inpToOutputMapping: RDD[(InputId, OutputId)] =
    base.flatMap(r => r._2.inputIds.map((_, r._1)))
    
    // Note: Although join can use a partitioner, the output of the join is keyed by InputId
    // which has no exploitable correlation. Thus, we don't get any benefit from specifying a
    // partitioner here (though we will partition later when reducing by key=OutputId).
    val firstJoin: RDD[RecursiveSchema] =
      LineageWrapper.joinLineageKeyedRDDs(inpToOutputMapping, first, useShuffle = true).values
    val firstAggLatenciesWithCounts: RDD[(OutputId, RmLatencyTupleWithCount)] =
      firstJoin.reduceByKeyWithCount(partitioner, aggFn)
    
    val resultWithoutShuffleStats: RDD[(OutputId, RmLatencyTupleWithCount)] = secondOption
    match {
      
      case Some(second) =>
        val secondJoin: RDD[RecursiveSchema] =
          LineageWrapper.joinLineageKeyedRDDs(inpToOutputMapping, second, useShuffle = true).values
        val secondAggLatenciesWithCounts: RDD[(OutputId, RmLatencyTupleWithCount)] =
          secondJoin.reduceByKeyWithCount(partitioner, aggFn)
      
        // Since inputs for each record in base are not guaranteed to appear in both RDDs, we
        // have to do the equivalent of a full outer join. At this point we also know both first
        // and second are uniquely keyed, so I optimize the fullOuterJoin code slightly.
        // In practice, we could also have included `base`, but it's not necessary due to the
        // assumption that every input record from `base` will show up in at least one of
        // first/second (ie a union of the OutputIds that appear in the first/second aggregated
        // latencies should equal the original set of OutputIds in `base`)
        // As both RDDs already use the same partitioner, this map-side 'reduce' should be
        // efficient.
        val totalAggregatedLatenciesWithCounts: RDD[(OutputId, RmLatencyTupleWithCount)] =
          firstAggLatenciesWithCounts.cogroup(secondAggLatenciesWithCounts,partitioner).map {
            case (outputId, aggCountIterTuple) => {
              val aggCount = aggCountIterTuple match {
                case (vs, Seq()) => vs.head
                case (Seq(), ws) => ws.head
                case (vs, ws) => vs.head.mergeWith(ws.head, aggFn)
              }
              (outputId, aggCount) // reordering/structuring schema.
            }
          }
      
        totalAggregatedLatenciesWithCounts
      case None =>
        // No second RDD to join, so we're done after the reduceByKey that's already been
        // applied. All we need to do is remap the output schema. Note that for these base
        // RDDs, we expect no partial latency for accumulation purposes.
        firstAggLatenciesWithCounts
    }
    // Use aggTapPartitionCount to avoid calling aggTap.getNumPartitions, which might rely on a
    // getPartitions implementation that is broken post-session and serialization
    val result = addLatencyStats(resultWithoutShuffleStats, aggDep)
    result
  }
  
  // see PerfTraceCalculatorV2 for potential optimizations, as this is essentially a fork
  def perfTraceRecursiveHelper(curr: LineageWrapper): RDD[RecursiveSchema] = {
    // re: generics usage, ideally we'd store an RDD[(ID, (_ <: CacheValue, Latency))] and convert
    // at  the end, but I'm not skilled enough with Scala generics (existential types??) to do
    // this. Instead, we just wrap/cast to PerfLineageCache at the end.
    val currTap = curr.tapName
    // First compute the latencies based on the predecessors, applying agg/acc as needed.
    val result: RDD[RecursiveSchema] = currTap match {
      case _ if currTap.isSourceTap =>
        if (!currTap.isHadoopTap) {
          throw new UnsupportedOperationException(s"Unsupported source tap RDD: $currTap")
        }
        
        type OutputValue = TapHadoopLRDDValue
        // TapHadoop has no predecessor, so it needs to build a new tuple instance.
        val result = curr.lineageCache.withValueType[OutputValue]
                                      .mapValues(SingleRmLatencyTuple.apply)
        result
        
      case _ if currTap.isStartOfStageTap && currTap.hasPredecessor =>
        // eg PostShuffle, PostCoGroup
        // This is the hardest case - there can be multiple dependencies, so we code
        // appropriately. Since we're dealing with RDD objects, it's safe to use a Seq and map
        // operations uniformly.
        type OutputValue = CacheValue // TODO specify this further if applicable
        val prevRdds: Seq[RDD[RecursiveSchema]] = prevPerfRDDs(curr)
        
        val currCache: RDD[(OutputId, OutputValue)] = curr.lineageCache.withValueType[OutputValue]
        
        // Generalized method to handle case when either one or two dependencies are present.
        val aggDeps = curr.lineageDependencies
        val result = joinInputRddsWithAggStats(
                                          aggDeps,
                                          currCache,
                                          prevRdds.head,
                                          prevRdds.lift(1)
        )
        result
      case _ if currTap.isEndOfStageTap =>
        // eg TapLRDD, PreShuffle, PreCoGroup
        // conveniently, we know there's only one input.
        type OutputValue = EndOfStageCacheValue
        val prevLatencyRdd: RDD[RecursiveSchema] = prevPerfRDD(curr)
        
        val currRdd: RDD[(OutputId, OutputValue)] = curr.lineageCache.withValueType[OutputValue]
        debugPrint(currRdd, "EndOfStage lineage prior to join with prev latencies - " + currTap)
        // Arrange the output rdd (currRdd) so that it can be joined against the inputs.
        val inpToOutputWithPartialLatency: RDD[(InputId, (OutputId, UnAccumulatedLatency))] =
          currRdd.flatMap(r => r._2.inputKeysWithPartialLatencies.map({
            case (inputId, partialLatency) => (inputId, (r._1, partialLatency)) // discard the
            // value after getting inputs
          }))
        val joinResult: RDD[(InputId, ((OutputId, UnAccumulatedLatency), SingleRmLatencyTuple))] =
        // TODO placeholder for future optimization - TapLRDDs should have 1-1 dependency on parents
          LineageWrapper.joinLineageKeyedRDDs(inpToOutputWithPartialLatency, prevLatencyRdd, useShuffle = false)
        
        // For TapLRDD, it's a 1-1 mapping so we only need to sum or otherwise accumulate latency.
        // No need to use the agg function.
        debugPrint(joinResult.sortBy(_._2._1._1), "EndOfStage join result (pre-accumulation, " +
          "may contain duplicates for shuffle-based RDDs) " + currTap)
        
        val accumulatedLatencies: RDD[RecursiveSchema] =
          if(currTap.isLatencyStatsTap) {
  
  
            // There are multiple latencies (computed from inputs) for each output record. We
            // have to aggregate over all of them after accumulating partial latencies.
            // jteoh: 10/17/2018 note - seems like I'm doing acc followed by agg here, which is
            // technically less CPU-efficient but allows removal of the partial latency earlier?
            val nonAggregatedLatencies: RDD[(OutputId, SingleRmLatencyTuple)] = // coincidentally
            // same as recursive schema, but we're not ready to use that yet.
            joinResult.values.map {
              case ((outputRecord, partialOutputLatency), inputLatencyTuple) =>
                (outputRecord, accFn(inputLatencyTuple, partialOutputLatency))
            }
            debugPrint(nonAggregatedLatencies, "EndOfStage post-accumulation, pre-aggregation " +
              "stats: " + currTap)
            //val reducePartitioner = new PRIdTuplePartitioner(nonAggregatedLatencies.getNumPartitions)
            val reducePartitioner =
              new PartitionWithRecIdPartitioner(nonAggregatedLatencies.getNumPartitions)
  
            val aggregatedLatenciesWithCounts: RDD[(OutputId, RmLatencyTupleWithCount)] =
              nonAggregatedLatencies.reduceByKeyWithCount(reducePartitioner, aggFn)
            debugPrint(aggregatedLatenciesWithCounts, "EndOfStage LatencyStatsTap computed " +
              "latencies post-aggregation, pre-shuffleStats: " + currTap)
            // TODO: In cases when there is no map-side combine, it is misleading to attribute
            // multiple inputs to the 'record' identified by the previous reduceByKey(agg)
            // operation. If no map-side combine exists, the # of outputs == # inputs.
            // Example scenario: groupByKey where a preshuffle partition consists only of 10
            // "foo"-keys. Each record technically contributes a fraction of the entire
            // partition latency, but the titian 'combined' lineage record will misleadingly
            // indicate otherwise.
            val result: RDD[RecursiveSchema] =
            // use curr.numPartitions because that's precomputed and saved, so no
            // post-serialization issues occur (vs using aggTap.getNumPartitions)
            addLatencyStats(aggregatedLatenciesWithCounts, curr.lineageDependencies)
            result
          } else {
            // There's no need to aggregate latencies (ie reduce), since TapLRDDValues have a strict
            // single input (1-1) mapping. Simply rearrange the records to the right schema.
            // We could still aggregate, but avoiding it is a performance optimization.
            joinResult.values.map {
              case ((outputId, partialOutputLatency), inputLatency) =>
                (outputId, accFn(inputLatency, partialOutputLatency))
            }
          }
        
        accumulatedLatencies
      
      case _ =>
        // This technically should never be reached, but if it does we're in trouble.
        throw new UnsupportedOperationException("Performance tracing is not supported for " +
                                                  currTap)
    }
    /*debugPrint(resultWithoutShuffleStats.sortBy(_._2._2, ascending = false),
               currTap + " computed latencies, without shuffle stats (if applicable)")*/
    
    debugPrint(result.sortBy(_._2.latency, ascending = false),
               currTap + " final computed latencies")
    result
  }
  private def addLatencyStats(rdd: RDD[(OutputId, RmLatencyTupleWithCount)],
                   aggDep: LineageCacheDependencies
                  ): RDD[RecursiveSchema] = {
    if(PerfDebugConf.get.estimateShuffleLatency) {
      accumulateLatencyStats(rdd, aggDep)
    } else {
      skipLatencyStats(rdd, aggDep)
    }
  }
  private def accumulateLatencyStats(
                                 rdd: RDD[(OutputId, RmLatencyTupleWithCount)],
                                 aggDep: LineageCacheDependencies
                                ): RDD[RecursiveSchema] = {
    val shuffleAggStats = getRDDAggStats(aggDep.tapRddId, aggDep.numPartitions)
    debugPrintStr(s"Shuffle agg stats for ${aggDep.tapStr}: (input/output/latency)" +
                    s"\n\t${shuffleAggStats.mkString("\n\t")}")
    val shuffleAggStatsBroadcast: Broadcast[Map[PartitionId, AggregateLatencyStats]] =
      initWrapper.context.broadcast(shuffleAggStats)
  
    
    val aggTap = aggDep.tapName
    // Check if the aggTap is a preshuffle Tap where map-side combine is not enabled - if that's
    // the case, we want to ignore the number of inputs because Titian only produces one output
    // record per key+partition in the preshuffle phase, and there's a strict 1-1 mapping if
    // map-side combine has been disabled (ie we know there's only one input).
    val mapSideCombineDisabled = aggTap.isEndOfStageTap && !aggDep.combinerEnabled
    // If we knew this result was partitioned by outputId.partition already, then we could
    // run a more efficient mapPartitions call with only one map lookup.
    // TODO: check presence of partitioner??
    val result = rdd.map {
      case (outputId, aggWithCount) => {
        val aggStats: AggregateLatencyStats = shuffleAggStatsBroadcast.value(outputId.partition)
        val numInputs = if (mapSideCombineDisabled) 1 else aggWithCount.count
        val latencyWithoutShuffle: SingleRmLatencyTuple = aggWithCount.aggResult
        // TODO: is precision loss a concern here? Using floats vs doubles (32 vs 64-bit) and
        //  rounding to nearest integer here.
        val shuffleLatency = // numOutputs is unused here.
          Math.round(aggStats.latency * (numInputs.toFloat /aggStats.numInputs.toFloat))
        (outputId, accFn(latencyWithoutShuffle, shuffleLatency))
      }
    }
    result
  }
  
  private def skipLatencyStats(
                                  rdd: RDD[(OutputId, RmLatencyTupleWithCount)],
                                  aggDep: LineageCacheDependencies
                              ): RDD[RecursiveSchema] = {
    // simplified version of above: ignores the aggregate latency stats altogether
    rdd.map {
      case(outputId, aggWithCount) => (outputId, aggWithCount.aggResult)
    }
  }
  
  // jteoh: copied and adapted for new RmLatTuple type on 10/17/2018
  // these could certainly be generalized later too.
  protected case class AggregatedResultWithCount[V](var aggResult: V, var count: Long) {
    def mergeWith(other: AggregatedResultWithCount[V], mergeFn: (V, V) => V): AggregatedResultWithCount[V] = {
      this.aggResult = mergeFn(this.aggResult, other.aggResult)
      this.count += other.count
      this
    }
  }
  
  // Note: These could be generalized (generics) but are only used for this specific use case
  // right now
  protected implicit class AggCountRDD[K,V](rdd: RDD[(K, V)])
                                           (implicit keyTag: ClassTag[K],
                                                     valueTag: ClassTag[V])
                                            extends Serializable {
    // TODO serializable is undesirable, but somewhere along the line this
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
                             fn: (V, V) => V): RDD[(K, AggregatedResultWithCount[V])] = {
      val (createCombinerAggCount, mergeValueAggCount, mergeCombinerAggCount) =
        createCombineFnsForReduce(fn)
      rdd.combineByKeyWithClassTag(createCombinerAggCount,
                                   mergeValueAggCount,
                                   mergeCombinerAggCount,
                                   part)
    }
    
    private def createCombineFnsForReduce(fn: (V, V) => V) = {
      // create V=> C - technically indep of fn
      val createCombinerAggCount = (v: V) => AggregatedResultWithCount(v, 1)
      // mergeV(C, V) => C
      val mergeValueAggCount = (c: AggregatedResultWithCount[V], v: V) => {
        c.aggResult = fn(c.aggResult, v)
        c.count += 1
        c
      }
      
      // mergeC(C, C) => C
      val mergeCombinerAggCount = (c1: AggregatedResultWithCount[V], c2: AggregatedResultWithCount[V]) => {
        c1.mergeWith(c2, fn)
      }
      
      (createCombinerAggCount, mergeValueAggCount, mergeCombinerAggCount)
    }
    
  }
}
