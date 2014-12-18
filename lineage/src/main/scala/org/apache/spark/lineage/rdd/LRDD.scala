package org.apache.spark.lineage.rdd

import org.apache.spark.lineage.{Lineage, LineageContext}
import org.apache.spark.rdd._
import org.apache.spark.{Dependency, OneToOneDependency}

import scala.reflect.ClassTag

abstract class LRDD[T: ClassTag](
  @transient private var lc: LineageContext, @transient deps: Seq[Dependency[_]])
  extends RDD[T](lc.sparkContext, deps) with Lineage[T] {

  /** Construct an LRDD with just a one-to-one dependency on one parent */
  def this(@transient prev: Lineage[_]) =
    this(prev.lineageContext , List(new OneToOneDependency(prev)))

  def lineageContext = lc
}
