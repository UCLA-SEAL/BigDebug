package org.apache.spark.lineage.perfdebug.lineageV2

import org.apache.spark.rdd.RDD

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