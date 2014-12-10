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

import org.apache.spark.{LocalityAwarePartitioner, Partition, TaskContext}

private[spark]
class ShowRDD(prev: RDD[((Int, Int, Long), String)])
  extends RDD[String](prev) {

  override def getPartitions: Array[Partition] = firstParent[((Int, Int, Long), Any)].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[((Int, Int, Long), String)].iterator(split, context).map(r => r._2)

  override def collect(): Array[String] = {
    val results = prev.context.runJob(
      prev.map(r => r._2), (iter: Iterator[String]) => iter.toArray.distinct
    )
    Array.concat(results: _*)
  }

  override def filter(f: String => Boolean): ShowRDD = {
    new ShowRDD(firstParent[((Int, Int, Long), String)].filter(r => f(r._2)).cache())
  }

  override def getLineage(): LineageRDD = {
    var shuffled: RDD[((Int, Int, Long), Any)] = prev.asInstanceOf[RDD[((Int, Int, Long), Any)]]
    if(prev.context.getCurrentLineagePosition.get.isInstanceOf[TapPreShuffleRDD[_]]) {
      val part = new LocalityAwarePartitioner(prev.context.getCurrentLineagePosition.get.partitions.size)
      shuffled = new ShuffledRDD[(Int, Int, Long), Any, Any](prev, part)
    }
    new LineageRDD(
      leftJoin(
        shuffled,
        prev.context.getCurrentLineagePosition.get.asInstanceOf[RDD[((Int, Int, Long), Any)]]
      ).cache()
    )
  }
}
