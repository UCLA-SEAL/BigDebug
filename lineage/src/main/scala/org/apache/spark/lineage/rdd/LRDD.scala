package org.apache.spark.lineage.rdd

import org.apache.spark.{OneToOneDependency, Dependency}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.rdd._

import scala.reflect._

abstract class LRDD[T: ClassTag](
  @transient private var lc: LineageContext, @transient deps: Seq[Dependency[_]])
  extends RDD[T](lc.sparkContext, deps) with Lineage[T]
{
  def this(@transient prev: Lineage[_]) =
    this(prev.lineageContext, List(new OneToOneDependency(prev)))

  override def lineageContext = lc

  override def ttag: ClassTag[T] = classTag[T]
}
