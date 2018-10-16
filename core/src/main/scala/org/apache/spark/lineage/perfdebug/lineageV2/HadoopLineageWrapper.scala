package org.apache.spark.lineage.perfdebug.lineageV2

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{PartitionWithRecId, TapHadoopLRDDValue}
import org.apache.spark.lineage.perfdebug.utils.PartitionWithRecIdPartitioner
import org.apache.spark.lineage.rdd.{HadoopLRDD, Lineage, MapPartitionsLRDD, TapHadoopLRDD}
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}

// Fragile class that basically assumes the tap is an instance of TapLRDD
class HadoopLineageWrapper(val lineageDependencies: LineageCacheDependencies,
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

object HadoopLineageWrapper {
  def apply(lineageDependencies: LineageCacheDependencies,
            lineageCache: LineageCache): HadoopLineageWrapper = {
    new HadoopLineageWrapper(lineageDependencies, lineageCache)
  }
}
