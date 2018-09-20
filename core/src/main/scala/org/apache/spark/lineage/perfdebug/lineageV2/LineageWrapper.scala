package org.apache.spark.lineage.perfdebug.lineageV2

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.lineage.perfdebug.perftrace._
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{PartitionWithRecId, TapHadoopLRDDValue}
import org.apache.spark.lineage.perfdebug.utils.{CacheDataTypes, PartitionWithRecIdPartitioner}
import org.apache.spark.lineage.perfdebug.utils.TapUtils._
import org.apache.spark.lineage.rdd.{TapHadoopLRDD, _}
import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.{Partitioner, SparkContext}

import scala.collection.mutable
import scala.reflect.ClassTag

/** Class that represents lineage data captured. The main entry point for acquiring an instance
 * of this class is the implicitly defined Lineage.lineageWrapper function.
 * @param lineageDependencies instance representing the current TapRDD + its lineage dependencies.
 * @param lineageCache the current cache (including any filtering that has been applied from
 *                     previous joins).
 */
class LineageWrapper protected(private val lineageDependencies: LineageCacheDependencies,
                               val lineageCache: LineageCache) extends {
  def tap: TapLRDD[_] = lineageDependencies.tap
  def lineageAppId: String = lineageDependencies.appId
  def context: SparkContext = lineageCache.sparkContext
  // used to avoid post-serialization issues, eg hadoop RDD's job conf being inaccessible
  def numPartitions: Int = lineageDependencies.numPartitions
  
  def dependencies: Seq[LineageCacheDependencies] = lineageDependencies.dependencies
  
  def hasParent(pos: Int = 0): Boolean = pos >= 0 && pos < dependencies.size
  
  def traceBackwards(pos: Int = 0): LineageWrapper = {
    if (!hasParent(pos)) {
      throw new UnsupportedOperationException(s"No parent at position $pos to trace to")
    }
    val parent = dependencies(pos)
    // start = postshuffle/postcogroup (and also sources, but lineage is not applicable there)
    // end = preshuffle, precogroup, tapLRDD - all retain the same partitioning.
    val useShuffle = parent.tap.isStartOfStageTap
  
    // Since the first RDD is keyed by identity, the key and value's first element are identical.
    val tracedParentCache = LineageWrapper.joinLineageKeyedRDDs(lineageCache.inputIds.keyBy(identity),
                                                        parent.fullLineageCache.rdd,
                                                        useShuffle).values
    LineageWrapper(parent, tracedParentCache)
  }
  
  /** Major assumption: only supported data source is hadoop. In the future,
   * we might be able to extend this to return InputLineageWrapper instead.
   * If there are multiple sources, this returns the leftmost one as defined by the first
   * dependency wherever applicable. This is essentially equivalent to [[traceBackAllSources().head]].*/
  def traceBackAll(): HadoopLineageWrapper = {
    var current = this
    while(current.hasParent()) {
      current = current.traceBackwards()
    }
    current.asInstanceOf[HadoopLineageWrapper]
  }
  
  /** Returns all determined data sources in the order that they appear via DF. Note that this
   * does not account for potential duplicates! */
  def traceBackAllSources():  Seq[HadoopLineageWrapper] = {
    // we know this has parents, ie isn't a data source.
    // Implement a depth-first search for all hadoop lineage wrappers
    val result = new CompactBuffer[HadoopLineageWrapper]()
    val stack = new mutable.Stack[LineageWrapper]()
    stack.push(this)
    while(stack.nonEmpty) {
      stack.pop() match {
        case hadoopLineage: HadoopLineageWrapper =>
          result += hadoopLineage
        case curr =>
          assert(curr.hasParent(), s"$curr non-hadoop lineage wrapper should not have dependencies!")
          stack.pushAll(
            curr.dependencies.indices.map(curr.traceBackwards).reverse // want to prepend
          )
          // if the above is confusing, see the pseudocode example at https://en.wikipedia.org/wiki/Depth-first_search#Example
          
      }
    }
    
    result
  }
  
  def filterId(fn: PartitionWithRecId => Boolean): LineageWrapper = {
    LineageWrapper(lineageDependencies, lineageCache.filter(r => fn(r._1)))
  }
  
  def tracePerformance(accFn: (Long, Long) => Long = _ +_,
                       aggFn: (Long, Long) => Long = Math.max,
                       printDebugging: Boolean = false,
                       printLimit: Option[Int] = None,
                       usePerfTraceCalculatorV2: Boolean = true): PerfLineageWrapper = {
    // TODO jteoh: Make this flag clearer/more easily customized.
    val calc: PerfTraceCalculator = usePerfTraceCalculatorV2 match {
      case true  => PerfTraceCalculatorV2(this, accFn, aggFn, printDebugging, printLimit)
      case false => PerfTraceCalculatorV1(this, accFn, aggFn, printDebugging, printLimit)
    }
    calc.calculate()
  }
  
  def printDependencies(showBefore: Boolean = false): Unit = lineageDependencies.print(showBefore)
  
  override def toString: String = s"${getClass.getSimpleName}($lineageDependencies," +
    s"$lineageCache)"
  
  /** Creates an instance of [[org.apache.spark.lineage.perfdebug.perftrace.DefaultPerfLineageWrapper]]
   * using the provided cache and the current wrapper's dependencies.
   * TODO jteoh: remove this coupling/move it to PerfLineageWrapper (need to scope the deps)
   */
  def asPerfLineageWrapper(perfCache: PerfLineageCache): DefaultPerfLineageWrapper = {
    DefaultPerfLineageWrapper(lineageDependencies, perfCache)
  }
  
  def asPerfLineageWrapper(idLatencyRDD: RDD[(PartitionWithRecId, Long)]): IdOnlyPerfLineageWrapper = {
    IdOnlyPerfLineageWrapper(lineageDependencies, idLatencyRDD, lineageCache)
  }
}

trait InputLineageWrapper[V] extends LineageWrapper {
  /** Filter the base input RDD using the associated lineage cache. In general, baseRDD should be
   *  exactly equivalent to the original input RDD (eg same hadoop file with same partitions). */
  def joinInputRDD(baseRDD: RDD[V]): RDD[V]
}

object InputLineageWrapper {
  /** Currently only supports Hadoop */
  def apply(lineageDependencies: LineageCacheDependencies,
            lineageCache: LineageCache): InputLineageWrapper[_] = {
    assert(lineageDependencies.dependencies.isEmpty, "Input lineage wrappers should not have tap " +
      "dependencies!")
    HadoopLineageWrapper(lineageDependencies, lineageCache)
  }
}

// Fragile class that basically assumes the tap is an instance of TapLRDD
case class HadoopLineageWrapper private(private val lineageDependencies: LineageCacheDependencies,
                                        override val lineageCache: LineageCache)
  extends LineageWrapper(lineageDependencies, lineageCache) with InputLineageWrapper[(Long,String)]{
  
  assert(lineageDependencies.tap.isInstanceOf[TapHadoopLRDD[_,_]])
  
  private def hadoopLineage: RDD[(PartitionWithRecId, TapHadoopLRDDValue)] =
    lineageCache.withValueType[TapHadoopLRDDValue]
  
  override def joinInputRDD(baseRDD: RDD[(Long, String)]): RDD[(Long, String)] = {
    // Implicit assumption: The base RDD partitions are exactly the same as original input, ie by
    // partition id. Thus we can do a partition-based join.
    val partitioner = new PartitionWithRecIdPartitioner(baseRDD.getNumPartitions)
    val partitionedHadoopLineage: RDD[Long] =
      hadoopLineage.values // TapHadoopLRDDValue instances
      .map(v => (v.outputId, v.byteOffset)) // byteOffset is unique within hadoopRDD partitions
      .partitionBy(partitioner) // organize by partition first
      .values // and extract byte offsets within each partition
      
    LineageWrapper.joinRightByPartitions(partitionedHadoopLineage, baseRDD)
  }
  
  def joinInputRDD(baseRDD: RDD[(LongWritable, Text)])
                  (implicit d: DummyImplicit): RDD[(Long, String)] = {
    // As is, the hadoop RDD is not serializable and will result in an error when joining. Thus,
    // we convert the writables to actual values.
    val rawHadoopDataRDDFixed: RDD[(Long, String)] =
      baseRDD.map({ case (lw, t) => (lw.get(), t.toString) })
    joinInputRDD(rawHadoopDataRDDFixed)
  }
  
  /** Convenience function because most data sources will start with output in the form of
   *  lineageContext.textFile(...). This function allows users to reuse the same RDD (from
   *  original session) or create a new one, assuming the same command is used.
   *
   *  Note: number of partitions must be the same as the original dataset!
   */
  def joinInputTextRDD(rdd: Lineage[String]): RDD[(Long, String)] = {
    val postHadoopMapRDD = rdd.asInstanceOf[MapPartitionsLRDD[String,(LongWritable, Text)]]
    val hadoopFileRDD: RDD[(LongWritable, Text)] = postHadoopMapRDD.prev match {
        // Need to handle the case when the RDD has been tapped.
      case tap: TapHadoopLRDD[_,_] =>
        tap.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
      case _: HadoopLRDD[_,_] =>
        postHadoopMapRDD.prev
    }
    
    joinInputRDD(hadoopFileRDD)
  }
  
  /** Convenience function because most data sources will start with output in the form of
   *  lineageContext.textFile(...). This function allows users to reuse the same RDD (from
   *  original session) or create a new one, assuming the same underlying key property is
   *  preserved. For hadoop RDDs, the 'key' is byte offset which is preserved.
   *  For those curious: context.textFile creates a hadoopRDD using Hadoop's TextInputFormat,
   *  which is inherently keyed by byte offsets with line splits.
   */
  def joinInputTextRDD(rdd: RDD[String]): RDD[(Long, String)] = {
    val postHadoopMapRDD = rdd.asInstanceOf[MapPartitionsRDD[String,(LongWritable, Text)]]
    val hadoopFileRDD: RDD[(LongWritable, Text)] = postHadoopMapRDD.prev
    joinInputRDD(hadoopFileRDD)
  }
  
  /** CAUTION - only use within the original spark session! */
  def rawInputRDD: RDD[(Long, String)] = {
    // really unclean approach to getting the original hadoop RDD WITH byte offsets, based on
    // LineageRDD line 303 (show() on TapHadoopLRDD)
    // Later this won't be usable as we'll need to recompute or persist this rdd for external usage.
    joinInputRDD(tap.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]])
  }
}

object LineageWrapper {
  def apply(lineageDependencies: LineageCacheDependencies,
            lineageCache: LineageCache): LineageWrapper = {
    if(lineageDependencies.dependencies.isEmpty) {
      InputLineageWrapper(lineageDependencies, lineageCache)
    } else {
      new LineageWrapper(lineageDependencies, lineageCache)
    }
  }
  
  def apply(lineageCacheDependencies: LineageCacheDependencies): LineageWrapper = {
      LineageWrapper(lineageCacheDependencies, lineageCacheDependencies.fullLineageCache)
  }
  
  def fromAppId(appId: String): LineageWrapper = {
    val deps = LineageCacheRepository.getCacheDependencies(appId)
    LineageWrapper(deps)
  }
  
  /** Central join method for joining two RDDs keyed by [[PartitionWithRecId]]. This
   * class is currently a placeholder for future optimization, but will hopefully be
   * be able to execute partition-based joins in the future. as well as potential broadcast
   * optimizations for small RDDs. */
  def joinLineageKeyedRDDs[V1, V2](child: RDD[(PartitionWithRecId, V1)],
                                   parent: RDD[(PartitionWithRecId, V2)],
                                   useShuffle: Boolean,
                                   partitioner: Option[Partitioner] = None)
                                  // need implicit classtags for automatic pairRDD conversion.
                                  (implicit vt1: ClassTag[V1],
                                  vt2: ClassTag[V2]
                                 ): RDD[(PartitionWithRecId, (V1, V2))] = {
    // TODO: future optimization
    if(!useShuffle && false) {
      assert(child.partitioner.isDefined && child.partitioner == parent.partitioner,
             "non-shuffle joins require predefined and identical partitioners on both rdds")
      // TODO - zipPartitions-based join? problem here is that some of the igniteRDDs won't come
      // out with a partitioner defined at the beginning. If it were defined, a normal join would
      // actually be pretty efficient thanks to the one to one dependencies.
      throw new UnsupportedOperationException("derp")
    } else {
      if(partitioner.isDefined) {
        child.join(parent, partitioner.get)
      }
      else {
        val p = new PartitionWithRecIdPartitioner(parent.getNumPartitions)
        child.join(parent, p)
      }
    }
  }
  
  /** Almost identical to Lineage.rightJoin - this assumes the two inputs can be joined
   * entirely within each partition (requiring no additional shuffle).
   */
  def joinRightByPartitions[T,V](prev: RDD[T], baseRDD: RDD[(T, V)]): RDD[(T,V)] = {
    prev.zipPartitions(baseRDD) {
      (buildIter, streamIter) =>
        val hashSet = new java.util.HashSet[T]()
        var rowKey: T = null.asInstanceOf[T]

        // Create a Hash set of buildKeys
        while (buildIter.hasNext) {
          rowKey = buildIter.next()
          val keyExists = hashSet.contains(rowKey)
          if (!keyExists) {
            hashSet.add(rowKey)
          }
        }

        if (hashSet.isEmpty) {
          Iterator.empty
        } else {
          streamIter.filter(current => {
            hashSet.contains(current._1)
          })
        }
    }
  }
  
  // Only useful within the spark session
  implicit class LineageWrappedRDD(rdd: Lineage[_]) {
    def lineageWrapper: LineageWrapper = {
      assert(rdd.getTap.isDefined, "Spark job must be executed first in order for lineage to be " +
        "recorded!")
      val dependencies = LineageCacheDependencies.buildLineageCacheDependencyTree(rdd.getTap.get)
      
      val appId = rdd.context.applicationId
      println("-" * 100)
      println("APP ID: " + appId)
      println("-" * 100)
      
      LineageWrapper(dependencies)
    }
  }
}