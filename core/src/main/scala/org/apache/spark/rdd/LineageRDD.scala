/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{Partition, TaskContext}

private[spark]
class LineageRDD(prev: RDD[((Int, Int, Long), Any)])
  extends RDD[(Int, Int, Long)](prev) {

  override def getPartitions: Array[Partition] = firstParent[((Int, Int, Long), Any)].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[((Int, Int, Long), Any)].iterator(split, context).map(r => r._1)

  override def collect(): Array[(Int, Int, Long)] = {
    val results = prev.context.runJob(
      prev.map(r => r._1).distinct(), (iter: Iterator[(Int, Int, Long)]) => iter.toArray
    )
    Array.concat(results: _*)
  }

  override def filter(f: ((Int, Int, Long)) => Boolean): LineageRDD = {
    new LineageRDD(firstParent[((Int, Int, Long), Any)].filter(r => f(r._1)))
  }

  def goNext(): LineageRDD = {
    new LineageRDD(
      new PairRDDFunctions[(Int, Int, Long), Any](prev)
      .join(prev.context.getForward)
      .distinct()
      .map(r => (r._2._2, r._1)))
  }

  def goBack(): LineageRDD = {
    val next = prev.context.getBackward
    if (next.isDefined) {
      new LineageRDD(
        new PairRDDFunctions[(Int, Int, Long), (Int, Int, Long)](
          next.get.asInstanceOf[RDD[((Int, Int, Long), (Int, Int, Long))]])
        .join(prev)
        .distinct()
        .map(r => (r._2._1, r._1)))
    } else {
      new LineageRDD(
        prev.asInstanceOf[RDD[((Int, Int, Long), (Int, Int, Long))]].map(r => (r._2, r._1))
      )
    }
  }

  def show(): ShowRDD = {
    val position = prev.context.getCurrentLineagePosition
    if(position.isDefined) {
      var result: ShowRDD = null
      if (position.get.isInstanceOf[TapHadoopRDD[_, _]]) {
        result = new ShowRDD (new PairRDDFunctions[Long, (Int, Int, Long)](
        new PairRDDFunctions[(Int, Int, Long), Any](prev)
          .join(position.get.asInstanceOf[RDD[((Int, Int, Long), (String, Long))]])
          .map(r => (r._2._2._2, r._1)).distinct())
          .join(position.get.firstParent.asInstanceOf[HadoopRDD[LongWritable, Text]]
            .map(r=> (r._1.get(), r._2.toString))).map(r => (r._2._1, r._2._2)))
      } else if(position.get.isInstanceOf[TapPreShuffleRDD[_]]) {
        result = new ShowRDD (
          new PairRDDFunctions[(Int, Int, Long), Any](prev)
            .join(position.get.asInstanceOf[TapPreShuffleRDD[_]]
              .getCached
              .asInstanceOf[RDD[(Any, (Any, (Int, Int, Long)))]]
            .map(r => (r._2._2, (r._1, r._2._1).toString()))
            ).map(r => (r._1, r._2._2)).distinct()
        )
      } else if(position.get.isInstanceOf[TapPostShuffleRDD[_]]) {
        result = new ShowRDD (
          new PairRDDFunctions[(Int, Int, Long), Any](prev)
            .join(position.get.asInstanceOf[TapPostShuffleRDD[_]]
            .getCached.setCaptureLineage(false)
            .asInstanceOf[RDD[((Any, Any), (Int, Int, Long))]]
            .map(r => (r._2, (r._1._1, r._1._2).toString()))
            ).map(r => (r._1, r._2._2)).distinct()
        )
      } else {
          throw new UnsupportedOperationException("what cache are you talking about?")
      }
      result.collect.foreach(println)
      result
    } else {
      throw new UnsupportedOperationException("what position are you talking about?")
    }
  }
}
