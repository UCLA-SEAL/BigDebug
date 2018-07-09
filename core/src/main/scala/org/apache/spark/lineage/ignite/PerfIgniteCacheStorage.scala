package org.apache.spark.lineage.ignite

import javax.cache.Cache
import org.apache.ignite.cache.query.ScanQuery
import org.apache.spark.lineage.ignite.CacheDataTypes.{PartitionWithRecId, PerfIgniteCacheValue, TapLRDDValue, TapPostShuffleLRDDValue, TapPreShuffleLRDDValue, _}
import org.apache.spark.lineage.rdd.{TapLRDD, TapPostShuffleLRDD, TapPreShuffleLRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.PackIntIntoLong
import org.apache.spark.util.collection.CompactBuffer

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.language.implicitConversions

abstract class PerfIgniteCacheStorage[V <: PerfIgniteCacheValue](val cacheArguments: CacheArguments,
                                            val conversionFn: Any => V) {
  val cache = IgniteCacheFactory.createIgniteCache[PartitionWithRecId, V](cacheArguments)
  def store(buffer: Array[Any]): Unit = {
    val data = buffer.map(r => {
      val rec = conversionFn(r)
      (rec.key, rec)
    }).toMap.asJava
    cache.putAll(data)
  }
}

object PerfIgniteCacheStorage {
  def store(appId: Option[String], rdd: RDD[_], data: Array[Any]): Unit = {
    doWithStorage(appId, rdd)(_.store(data))
  }
  
  private def getValuesIterator[T <: PerfIgniteCacheValue](appId: Option[String],
                                                           rdd: RDD[_]): Iterable[T] = {
    doWithStorage(appId, rdd) {
      storage: PerfIgniteCacheStorage[_] =>
        val cache = storage.cache
        val cursor = cache.query[Cache.Entry[PartitionWithRecId, T]](new ScanQuery(null))
        cursor.asScala.map(_.getValue)
    }.getOrElse(Iterable.empty)
  }
  
  def print(appId: Option[String], rdd: RDD[_], topN: Int = 15): Unit = {
    println(s"Printing contents for rdd ${rdd.getClass.getSimpleName}[${rdd.id}] in " +
      s"cache ${buildCacheName(appId, rdd)}")
    type PartitionId = Int
    type OutputPartitionId = PartitionId
    type InputPartitionId = PartitionId
    type RecId = Int
    type OutputRecId = RecId
    type InputRecId = RecId
    type LatencyNs = Long
    rdd match {
      case _ : TapPreShuffleLRDD[_] =>
        val values = getValuesIterator[TapPreShuffleLRDDValue](appId, rdd)
        println("TapPreShuffleLRDD Schema: ((OutputPartitionId, OutputRecId), [InputRecId*], " +
          "[OutputTimestamp*], [OutputLatency*])")
        values.take(topN).foreach( value =>
            println(s"\t(${value.outputId.asTuple}, [${value.inputIds.mkString(",")}], " +
              s"[${value.outputTimestamps.mkString(",")}], [${value.outputRecordLatencies
                .mkString(",")}])")
        )

      case _ : TapPostShuffleLRDD[_] =>
        val values = getValuesIterator[TapPostShuffleLRDDValue](appId, rdd)
        println("TapPostShuffleLRDD Schema: ((OutputPartitionId, OutputRecId), CBuff, buffKey)??")
        values.take(topN).foreach(
        println
        )

      case _ : TapLRDD[_] =>
        val values = getValuesIterator[TapLRDDValue](appId, rdd)
        // inefficient materialization to list and sort by descending time, then take top few tuples
        val records: immutable.Seq[((OutputPartitionId, OutputRecId),
          (InputPartitionId, InputRecId),
          LatencyNs)] =
        // Key is same as value, so toss it out
          values.toList
            .sortBy(-_.latency)
            .map(r => {
              (r.outputId.asTuple,
                r.inputId.asTuple,
                r.latency)
            })
            .take(topN)
      
        println("TapLRDD Schema: " +
          "((OutputPartitionId, OutputRecId), (InputPartitionId,InputRecId), LatencyNs)")
        records.foreach(r => println(s"\t$r"))
      case _ =>
        println(s"Warning: no ignite storage available for non-tapped RDD $rdd")
    }
  }
  
  // Template
  private def doWithStorage[T](appId: Option[String],
                            rdd: RDD[_])
                           (fn: PerfIgniteCacheStorage[_] => T): Option[T] = {
    val cacheName = buildCacheName(appId, rdd)
    val cacheArgs = CacheArguments(cacheName)
    val storageConstructor: Option[CacheArguments => PerfIgniteCacheStorage[_]] = getStorageConstructor(rdd)
    
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
  PerfIgniteCacheStorage[TapLRDDValue](cacheArguments, (r: Any) => {
    val tuple = r.asInstanceOf[(Long, Long,Long)]
    // implicitly rely on conversions to proper data types here, rather than using
    // `tupled` and using native types
    TapLRDDValue(tuple._1, tuple._2, tuple._3)
  })

final class TapPreShuffleLRDDIgniteStorage(override val cacheArguments: CacheArguments) extends
  PerfIgniteCacheStorage[TapPreShuffleLRDDValue](cacheArguments, (r: Any) => {
    val tuple = r.asInstanceOf[((Int, Int), Array[Int], List[Long], List[Long])]
    TapPreShuffleLRDDValue((PackIntIntoLong.apply _).tupled(tuple._1), tuple._2, tuple._3, tuple._4)
  })

final class TapPostShuffleLRDDIgniteStorage(override val cacheArguments: CacheArguments) extends
  PerfIgniteCacheStorage[TapPostShuffleLRDDValue](cacheArguments, (r: Any) => {
  val tuple = r.asInstanceOf[(Long, (CompactBuffer[Long], Int))]
  TapPostShuffleLRDDValue(tuple._1, tuple._2._1, tuple._2._2)
})
