package org.apache.spark.lineage.rdd

import org.apache.spark.OneToOneDependency
import org.apache.spark.lineage.LineageContext
import org.apache.spark.rdd.ParallelCollectionRDD

import scala.collection.Map
import scala.reflect._

private[spark] class ParallelCollectionLRDD[T: ClassTag](
    @transient lc: LineageContext,
    @transient data: Seq[T],
    numSlices: Int,
    locationPrefs: Map[Int, Seq[String]]
  )extends ParallelCollectionRDD[T](lc.sparkContext, data, numSlices, locationPrefs) with Lineage[T]
{
  override def lineageContext = lc

  override def ttag: ClassTag[T] = classTag[T]

  override def tapRight(): TapLRDD[T] = {
    val tap = new TapParallelCollectionLRDD[T](lineageContext,  Seq(new OneToOneDependency(this)))
    setTap(tap)
    setCaptureLineage(true)
    tap
  }
}