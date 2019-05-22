package org.apache.spark.lineage.perfdebug.lineageV2

import org.apache.spark.lineage.perfdebug.ignite.lineageV2.IgniteLineageRecordsStorage
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes._
import org.apache.spark.lineage.rdd._
import org.apache.spark.rdd.RDD

/**
 * Storage object designed to support storing and retrieving lineage + performance data with a
 * configured storage system, e.g. Ignite. Note that the getValuesIterator method is provided
 * more for validation than optimized performance - in practice, it's more efficient to utilize
 * RDDs via a separate API. This overlaps a fair amount with [[org.apache.spark.lineage.perfdebug
 * .perftrace.LineageCacheRepository]] but is primarily focused with individual records, rather
 * than whole RDDs. (This is for functionality within LineageManager/tasks/executors)
 */
trait LineageRecordsStorage {
  def store(appId: String, rdd: RDD[_], data: Array[Any]): Unit
  def getValuesIterator[T <: CacheValue](appId: String,
                                         rdd: RDD[_]): Iterable[T]
  /* Utility method to help with printing with schema - in practice, you can also use
  getValuesIterator or the ignite RDD and rely on the default toString for the values
   */
  def print(appId: String, rdd: RDD[_], topN: Int = 15): Unit = {
    println(s"Printing contents for rdd ${rdd.getClass.getSimpleName}[${rdd.id}] in " +
              s"cache ${buildCacheName(appId, rdd)}")
    rdd match {
      case _ : TapPreCoGroupLRDD[_] =>
        val values = getValuesIterator[TapPreCoGroupLRDDValue](appId, rdd)
        println("TapPreCoGroupLRDD Schema: " + TapPreCoGroupLRDDValue.readableSchema)
        values.take(topN).foreach(v => println("\t" + v))
      case _ : TapPostCoGroupLRDD[_] =>
        val values = getValuesIterator[TapPostCoGroupLRDDValue](appId, rdd)
        println("TapPostCoGroupLRDD Schema: " + TapPostCoGroupLRDDValue.readableSchema)
        values.take(topN).foreach(v => println("\t" + v))
    
      case _ : TapPreShuffleLRDD[_] =>
        val values = getValuesIterator[TapPreShuffleLRDDValue](appId, rdd)
        println("TapPreShuffleLRDD Schema: " + TapPreShuffleLRDDValue.readableSchema)
        values.take(topN).foreach(v => println("\t" + v))
      case _ : TapPostShuffleLRDD[_] =>
        val values = getValuesIterator[TapPostShuffleLRDDValue](appId, rdd)
        println("TapPostShuffleLRDD Schema: " + TapPostShuffleLRDDValue.readableSchema)
        values.take(topN).foreach(v => println("\t" + v))
    
      case _ : TapHadoopLRDD[_,_] =>
        val values = getValuesIterator[TapHadoopLRDDValue](appId, rdd)
        println("TapHadoopLRDD Schema: " + TapHadoopLRDDValue.readableSchema)
        values.toList.sortBy(_.byteOffset)
        .take(topN).foreach(v => println("\t" + v))
    
      case _ : TapLRDD[_] =>
        val values = getValuesIterator[TapLRDDValue](appId, rdd)
        // inefficient materialization to list and sort by descending time, then take top few tuples
        println("TapLRDD Schema: " + TapLRDDValue.readableSchema)
        values.toList.sortBy(-_.partialLatencies.head)
        .take(topN)
        .foreach(v => println("\t" + v))
    
      case _ =>
        println(s"Warning: no ignite storage available for non-tapped RDD $rdd")
    }
  }
  
  def buildCacheName(appId: String, rdd: RDD[_]): String = {
    buildCacheName(appId, rdd.id, rdd.getClass.getSimpleName)
  }
  
  def buildCacheName(appId: String, rddId: Int, rddName: String): String = {
    s"${appId}_$rddId[$rddName]"
  }
}


object LineageRecordsStorage {
  // TODO make this configurable via conf in the future. Also consider integrating with SparkEnv
  private var _instance: Option[LineageRecordsStorage] = None
  def getInstance(): LineageRecordsStorage = {
    if(_instance.isEmpty) {
      // TODO remove default instantiation eventually
      _instance = Some(IgniteLineageRecordsStorage)
    }
    _instance.getOrElse(
      throw new IllegalStateException("No PerfLineageCacheStorage instance has been set. Did you " +
                                        "mean to call PerfLineageCacheStorage.setInstance(...)?")
    )
  }
  def setInstance(instance: LineageRecordsStorage) =
    _instance = Option(instance)
}
