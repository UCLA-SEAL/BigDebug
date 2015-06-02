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

package org.apache.spark.lineage.rdd

import org.apache.spark.lineage.LocalityAwarePartitioner
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.reflect._

private[spark]
class ShowRDD(prev: Lineage[(RecordId, String)])
  extends RDD[String](prev) with Lineage[String]
{
  override def ttag = classTag[String]

  override def lineageContext = prev.lineageContext

  override def getPartitions: Array[Partition] = firstParent[(RecordId, Any)].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[(RecordId, String)].iterator(split, context).map(r => r._2)

  override def collect(): Array[String] = {
    val results = prev.context.runJob(
      prev.map(r => r._2), (iter: Iterator[String]) => iter.toArray.distinct
    ).filter(_ != null)
    Array.concat(results: _*)
  }

  override def filter(f: String => Boolean): ShowRDD =
    new ShowRDD(firstParent[(RecordId, String)].filter(r => f(r._2)).cache())

  override def getLineage(): LineageRDD = {
    val position = prev.lineageContext.getCurrentLineagePosition.get
    position match {
      case _: TapParallelCollectionLRDD[_] => new LineageRDD(position.asInstanceOf[Lineage[(_, _)]])
      case _ => {
        var right = position
        val shuffled: Lineage[(RecordId, Any)] = position match {
          case _: TapPreShuffleLRDD[_] =>
            val part = new LocalityAwarePartitioner(position.partitions.size)
            new ShuffledLRDD[RecordId, Any, Any](prev.asInstanceOf[Lineage[(RecordId, Any)]], part)
          case _: TapPostCoGroupLRDD[_] =>
            val part = new LocalityAwarePartitioner(position.partitions.size)
            right = new ShuffledLRDD[RecordId, Any, Any](position.map {
                case (a, b) => (b.asInstanceOf[RecordId], a)
              }, part).setMapSideCombine(false)
            prev.asInstanceOf[Lineage[(RecordId, Any)]]
          case _ => prev.asInstanceOf[Lineage[(RecordId, Any)]]
        }

        var join = rightJoin(
          shuffled,
          right.asInstanceOf[Lineage[(RecordId, Any)]]
        )
        join = position match {
          case _: TapPostCoGroupLRDD[_] => join.map(r => (r._2, r._1)).cache()
          case _ => join.cache()
        }
        new LineageRDD(join.asInstanceOf[Lineage[(Any, Any)]])
      }
    }
  }

  def dump: RDD[(RecordId, String)] = prev
}
