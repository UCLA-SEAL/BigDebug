package org.apache.spark.lineage.ignite

import javax.cache.Cache
import org.apache.ignite.cache.query.ScanQuery
import org.apache.spark.lineage.ignite.CacheDataTypes.{PartitionWithRecId, CacheValue, TapLRDDValue, TapPostShuffleLRDDValue, TapPreShuffleLRDDValue, _}
import org.apache.spark.lineage.rdd.{TapLRDD, TapPostShuffleLRDD, TapPreShuffleLRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.PackIntIntoLong
import org.apache.spark.util.collection.CompactBuffer

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.language.implicitConversions

/** Central class for binding [[TapLRDD]] and its subclasses with their
 * [[CacheValue]] and ignite cache classes (this).
 * This class also manages the conversion of records (from materialized buffers in the RDDs) to
 * the corresponding [[CacheValue]], and may contain additional logic than that
 * found in the Titian codebase.
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
  val cache = IgniteCacheFactory.createIgniteCache[PartitionWithRecId, V](cacheArguments)
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

object PerfIgniteCacheStorage {
  def store(appId: Option[String], rdd: RDD[_], data: Array[Any]): Unit = {
    doWithStorage(appId, rdd)(_.store(data))
  }
  
  def getValuesIterator[T <: CacheValue](appId: Option[String],
                                         rdd: RDD[_]): Iterable[T] = {
    doWithStorage(appId, rdd) {
      storage: PerfIgniteCacheStorage[_,_] =>
        val cache = storage.cache
        val cursor = cache.query[Cache.Entry[PartitionWithRecId, T]](new ScanQuery(null))
        cursor.asScala.map(_.getValue)
    }.getOrElse(Iterable.empty)
  }
  
  /* Utility method to help with printing with schema - in practice, you can also use
  getValuesIterator or the ignite RDD and rely on the default toString for the values
   */
  def print(appId: Option[String], rdd: RDD[_], topN: Int = 15): Unit = {
    println(s"Printing contents for rdd ${rdd.getClass.getSimpleName}[${rdd.id}] in " +
      s"cache ${buildCacheName(appId, rdd)}")
    rdd match {
      case _ : TapPreShuffleLRDD[_] =>
        val values = getValuesIterator[TapPreShuffleLRDDValue](appId, rdd)
        println("TapPreShuffleLRDD Schema: " + TapPreShuffleLRDDValue.readableSchema)
        values.take(topN).foreach(v => println("\t" + v))

      case _ : TapPostShuffleLRDD[_] =>
        val values = getValuesIterator[TapPostShuffleLRDDValue](appId, rdd)
        println("TapPostShuffleLRDD Schema: " + TapPostShuffleLRDDValue.readableSchema)
        values.take(topN).foreach(v => println("\t" + v))

      case _ : TapLRDD[_] =>
        val values = getValuesIterator[TapLRDDValue](appId, rdd)
        // inefficient materialization to list and sort by descending time, then take top few tuples
        println("TapLRDD Schema: " + TapLRDDValue.readableSchema)
        values.toList.sortBy(-_.latency)
          .take(topN)
          .foreach(v => println("\t" + v))
      
      case _ =>
        println(s"Warning: no ignite storage available for non-tapped RDD $rdd")
    }
  }
  
  // Template
  def doWithStorage[T](appId: Option[String],
                            rdd: RDD[_])
                           (fn: PerfIgniteCacheStorage[_,_] => T): Option[T] = {
    val cacheName = buildCacheName(appId, rdd)
    val cacheArgs = CacheArguments(cacheName)
    val storageConstructor: Option[CacheArguments => PerfIgniteCacheStorage[_,_]] =
      getStorageConstructor(rdd)
    
    // Returns none if no storage constructor defined
    storageConstructor.map(constructor => fn(constructor(cacheArgs)))
  }
  
  private def getStorageConstructor(rdd: RDD[_]) = {
    rdd match {
      case _: TapPreShuffleLRDD[_] =>
        Some(new TapPreShuffleLRDDIgniteStorage(_))
      case _: TapPostShuffleLRDD[_] =>
        Some(new TapPostShuffleLRDDIgniteStorage(_))
      case _: TapLRDD[_] =>
        Some(new TapLRDDIgniteStorage(_))
      case _ =>
        println(s"Warning: no ignite storage available for non-tapped RDD $rdd")
        None
    }
  }
  
  def buildCacheName(appId: Option[String], rdd: RDD[_]) = {
    // TODO set up cache name properly
    s"${appId.get}_${rdd.id}"
  }
}

final class TapLRDDIgniteStorage(override val cacheArguments: CacheArguments) extends
  PerfIgniteCacheStorage[TapLRDDValue, TapLRDD[_]](
    cacheArguments,
    TapLRDDValue.fromRecord
  )

final class TapPreShuffleLRDDIgniteStorage(override val cacheArguments: CacheArguments) extends
  PerfIgniteCacheStorage[TapPreShuffleLRDDValue, TapPreShuffleLRDD[_]](
    cacheArguments,
    TapPreShuffleLRDDValue.fromRecord
  )

final class TapPostShuffleLRDDIgniteStorage(override val cacheArguments: CacheArguments) extends
  PerfIgniteCacheStorage[TapPostShuffleLRDDValue, TapPostShuffleLRDD[_]](
    cacheArguments,
    TapPostShuffleLRDDValue.fromRecord
  )
