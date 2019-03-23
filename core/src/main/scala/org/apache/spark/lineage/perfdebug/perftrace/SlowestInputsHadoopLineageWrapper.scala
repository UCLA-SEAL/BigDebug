package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.Latency
import org.apache.spark.lineage.perfdebug.lineageV2.{HadoopLineageWrapper, LineageCacheDependencies, LineageWrapper}
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes.{CacheValue, TapHadoopLRDDValue}
import org.apache.spark.lineage.perfdebug.utils.PartitionWithRecIdPartitioner
import org.apache.spark.lineage.rdd.{HadoopLRDD, Lineage, MapPartitionsLRDD, TapHadoopLRDD}
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}

class SlowestInputsHadoopLineageWrapper(override val lineageDependencies: LineageCacheDependencies,
                                        val valuesWithLatencyEstimates: RDD[(TapHadoopLRDDValue, Latency)])
  extends HadoopLineageWrapper(lineageDependencies,
                               valuesWithLatencyEstimates.keys.map(v => (v.key, v.asInstanceOf[CacheValue]))){
  type Offset = Long
  
  private val inputOrdering: Ordering[(TapHadoopLRDDValue, Latency)] = Ordering.by(_._2)
  
  def top(n: Int): SlowestInputsHadoopLineageWrapper = {
    val topInputs = valuesWithLatencyEstimates.top(n)(inputOrdering)
    val slowestInputsRDD: RDD[(TapHadoopLRDDValue, Latency)] = context.parallelize(topInputs)
    new SlowestInputsHadoopLineageWrapper(lineageDependencies, slowestInputsRDD)
  }
  
  // Utilities for joining and presenting the latency ranking. baseRDD is an RDD keyed by offset
  // (ie hadoop rdds)
  def joinInputRDDWithLatencyEstimate(baseRDD: RDD[(Long, String)])
                                      : RDD[(Long, (String, Latency))] = {
    // Implicit assumption: The base RDD partitions are exactly the same as original input, ie by
    // partition id. Thus we can do a partition-based join.
    val partitioner = new PartitionWithRecIdPartitioner(baseRDD.getNumPartitions)
    val partitionedHadoopLineage: RDD[(Offset, Latency)] =
      valuesWithLatencyEstimates.map({
        case (hadoopValue, score) => (hadoopValue.outputId, (hadoopValue.byteOffset, score)) })
      // byteOffset is unique within hadoopRDD partitions, so we'll use that as key after
      // partitioning
      .partitionBy(partitioner) // organize by partition first
      .values // and extract byte offsets within each partition
    
    // LineageWrapper.joinRightByPartitions(partitionedHadoopLineage, baseRDD)
    
    // Execution a partition-based join. This is required because there's no explicit way to
    // override the partitioner field in Spark without actually repartitioning, but we have
    // developer knowledge that this is already the case. This is extremely similar to
    // LineageWrapper.joinRightByPartitions
    partitionedHadoopLineage.zipPartitions(baseRDD) { (buildIter, streamIter) =>
          val slowestRecordsMap = buildIter.toMap
          if(slowestRecordsMap.isEmpty) {
            Iterator.empty
          } else {
            streamIter.flatMap({case (offset, text) =>
              slowestRecordsMap.get(offset).map(score => (offset, (text, score)))
              // fun scala: this is either an empty or singleton because get
              // returns an option!
            })
          }
    }
  }
  
  def joinInputRDDWithRankScore(baseRDD: RDD[(LongWritable, Text)])(implicit d: DummyImplicit)
                                                              : RDD[(Long, (String, Latency))] = {
    // As is, the hadoop RDD is not serializable and will result in an error when joining. Thus,
    // we convert the writables to actual values.
    val rawHadoopDataRDDFixed: RDD[(Long, String)] =
      baseRDD.map({ case (lw, t) => (lw.get(), t.toString) })
    joinInputRDDWithLatencyEstimate(rawHadoopDataRDDFixed)
  }
  
  /** Convenience function because most data sources will start with output in the form of
   *  lineageContext.textFile(...). This function allows users to reuse the same RDD (from
   *  original session) or create a new one, assuming the same command is used.
   *
   *  Note: number of partitions must be the same as the original dataset!
   */
  def joinInputTextRDDWithRankScore(rdd: Lineage[String]): RDD[(Long, (String, Latency))] = {
    val postHadoopMapRDD = rdd.asInstanceOf[MapPartitionsLRDD[String,(LongWritable, Text)]]
    val hadoopFileRDD: RDD[(LongWritable, Text)] = postHadoopMapRDD.prev match {
      // Need to handle the case when the RDD has been tapped vs an external query (without)
      case tap: TapHadoopLRDD[_,_] =>
        tap.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
      case _: HadoopLRDD[_,_] =>
        postHadoopMapRDD.prev
    }
  
    joinInputRDDWithRankScore(hadoopFileRDD)
  }
  
  /** Convenience function because most data sources will start with output in the form of
   *  lineageContext.textFile(...). This function allows users to reuse the same RDD (from
   *  original session) or create a new one, assuming the same underlying key property is
   *  preserved. For hadoop RDDs, the 'key' is byte offset which is preserved.
   *  For those curious: context.textFile creates a hadoopRDD using Hadoop's TextInputFormat,
   *  which is inherently keyed by byte offsets with line splits.
   */
  def joinInputTextRDDWithRankScore(rdd: RDD[String]): RDD[(Long, (String, Latency))] = {
    val postHadoopMapRDD = rdd.asInstanceOf[MapPartitionsRDD[String,(LongWritable, Text)]]
    val hadoopFileRDD: RDD[(LongWritable, Text)] = postHadoopMapRDD.prev
    joinInputRDDWithRankScore(hadoopFileRDD)
  }
  
  
}
