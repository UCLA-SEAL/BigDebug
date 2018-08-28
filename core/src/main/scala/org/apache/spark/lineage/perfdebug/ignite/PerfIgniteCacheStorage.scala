package org.apache.spark.lineage.perfdebug.ignite

import javax.cache.Cache
import org.apache.ignite.cache.query.ScanQuery
import org.apache.spark.lineage.perfdebug.storage.CacheDataTypes.{CacheValue, PartitionWithRecId, TapLRDDValue, TapPostShuffleLRDDValue, TapPreShuffleLRDDValue, _}
import org.apache.spark.lineage.perfdebug.storage.PerfLineageCacheStorage
import org.apache.spark.lineage.perfdebug.storage.{CacheArguments, PerfLineageCacheStorage}
import org.apache.spark.lineage.rdd._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.language.implicitConversions

/** Central class for binding [[TapLRDD]] and its subclasses with their
 * [[CacheValue]] and ignite cache classes (this).
 * This class also manages the conversion of records (from materialized buffers in the RDDs) to
 * the corresponding [[CacheValue]], and may contain additional logic than that
 * found in the Titian codebase.
 *
 * This class is strictly concerned with the storage of lineage data + individual record latency.
 * For approximated latency per partition, see [[IgniteCacheAggregateStatsStorage]].
 *
 * @param cacheArguments ignite cache configuration arguments
 * @param conversionFn   function to convert records to the corresponding [[CacheValue]]
 * @tparam V type of [[CacheValue]] to store. Should match with [[B]]
 * @tparam B type of [[TapLRDD]] that this cache should be used with. While not explicitly
 *           utilized in code, this is included for a clearer dependence mapping.
 */
abstract class PerfIgniteCacheStorage[V <: CacheValue,
                                      B <: TapLRDD[_]] ( // B used for tracking code, but not
                                                         // required at runtime
                                            val cacheArguments: CacheArguments,
                                            val conversionFn: Any => V) {
  // Possible optimization later - look into comment for keepPartitions
  val cache = IgniteCacheFactory.createIgniteCacheWithPRKey[V](cacheArguments,keepPartitions=false)
  def store(buffer: Array[Any]): Unit = {
    val data = buffer.map(r => {
      val rec = conversionFn(r)
      (rec.key, rec)
    }).toMap.asJava
    cache.putAll(data)
  }
  
  def get = cache.get _
  def getAll = cache.getAll _
}

object PerfIgniteCacheStorage extends PerfLineageCacheStorage {
  override def store(appId: String, rdd: RDD[_], data: Array[Any]): Unit = {
    doWithStorage(appId, rdd)(_.store(data))
  }
  
  override def getValuesIterator[T <: CacheValue](appId: String,
                                         rdd: RDD[_]): Iterable[T] = {
    doWithStorage(appId, rdd) {
      storage: PerfIgniteCacheStorage[_,_] =>
        val cache = storage.cache
        val cursor = cache.query[Cache.Entry[PartitionWithRecId, T]](new ScanQuery(null))
        cursor.asScala.map(_.getValue)
    }.getOrElse(Iterable.empty)
  }
  
  // Template
  private def doWithStorage[T](appId: String,
                               rdd: RDD[_])
                              (fn: PerfIgniteCacheStorage[_,_] => T): Option[T] = {
    val cacheName = buildCacheName(appId, rdd)
    val cacheArgs = CacheArguments(cacheName, rdd.getNumPartitions)
    val storageConstructor: Option[CacheArguments => PerfIgniteCacheStorage[_,_]] =
      getStorageConstructor(rdd)
    
    // Returns none if no storage constructor defined
    storageConstructor.map(constructor => fn(constructor(cacheArgs)))
  }
  
  private def getStorageConstructor(rdd: RDD[_]
                                   ): Option[CacheArguments => PerfIgniteCacheStorage[_,_]] = {
    rdd match {
      case _: TapPreCoGroupLRDD[_] =>
        Some(new TapPreCoGroupLRDDIgniteStorage(_))
      case _: TapPostCoGroupLRDD[_] =>
        Some(new TapPostCoGroupLRDDIgniteStorage(_))
      
      case _: TapPreShuffleLRDD[_] =>
        Some(new TapPreShuffleLRDDIgniteStorage(_))
      case _: TapPostShuffleLRDD[_] =>
        Some(new TapPostShuffleLRDDIgniteStorage(_))
      
      case _: TapHadoopLRDD[_,_] =>
        Some(new TapHadoopLRDDIgniteStorage(_))
      
      case _: TapLRDD[_] => // needs to be at the end because all others extend from this
        Some(new TapLRDDIgniteStorage(_))
      
      case _ =>
        println(s"Warning: no ignite storage available for non-tapped RDD $rdd")
        None
    }
  }
}

private final class TapLRDDIgniteStorage(override val cacheArguments: CacheArguments)
  extends PerfIgniteCacheStorage[TapLRDDValue, TapLRDD[_]](
    cacheArguments,
    TapLRDDValue.fromRecord
  )

private final class TapHadoopLRDDIgniteStorage(override val cacheArguments: CacheArguments)
  extends PerfIgniteCacheStorage[TapHadoopLRDDValue, TapHadoopLRDD[_,_]](
    cacheArguments,
    TapHadoopLRDDValue.fromRecord
  )

private final class TapPreShuffleLRDDIgniteStorage(override val cacheArguments: CacheArguments)
  extends PerfIgniteCacheStorage[TapPreShuffleLRDDValue, TapPreShuffleLRDD[_]](
    cacheArguments,
    TapPreShuffleLRDDValue.fromRecord
  )

private final class TapPostShuffleLRDDIgniteStorage(override val cacheArguments: CacheArguments)
  extends PerfIgniteCacheStorage[TapPostShuffleLRDDValue, TapPostShuffleLRDD[_]](
    cacheArguments,
    TapPostShuffleLRDDValue.fromRecord
  )

private final class TapPreCoGroupLRDDIgniteStorage(override val cacheArguments: CacheArguments)
  extends PerfIgniteCacheStorage[TapPreCoGroupLRDDValue, TapPreCoGroupLRDD[_]](
    cacheArguments,
    TapPreCoGroupLRDDValue.fromRecord
  )

private final class TapPostCoGroupLRDDIgniteStorage(override val cacheArguments: CacheArguments)
  extends PerfIgniteCacheStorage[TapPostCoGroupLRDDValue, TapPostCoGroupLRDD[_]](
    cacheArguments,
    TapPostCoGroupLRDDValue.fromRecord
  )