package org.apache.spark.lineage.iterative

import org.apache.spark.lineage.LineageContext.RecordId
import org.apache.spark.lineage.rdd.MapPartitionsLRDD
import org.apache.spark.{TaskContext, TaskContextImpl}
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.util.PackIntIntoLong

import scala.reflect.ClassTag

// simple mapPartitionsRDD that extracts input ids from context and produces them as output.
class IterationStartLineageRDD[T: ClassTag](prev: IterationStartRDD[T])
  extends MapPartitionsLRDD[RecordId, T](prev,
                                    {
                                      case (context, split, iter) =>
                                        val tContext = context.asInstanceOf[TaskContextImpl]
                                        iter.map(_ => {
                                          // assumption: the source iterator will be implicitly updating tcontext current input id
                                          (split, tContext.currentInputId)
                                        })
                                    }, true) {
  this.setName("IterationStart's Lineage Buffer")
}
