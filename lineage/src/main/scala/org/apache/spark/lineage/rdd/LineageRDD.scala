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

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark._
import org.apache.spark.lineage.Direction.Direction
import org.apache.spark.lineage.Direction.Direction
import org.apache.spark.lineage.{Direction, Lineage}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.CompactBuffer

import scala.reflect._

private[spark]
class LineageRDD(prev: Lineage[((Int, Int, Long), Any)])
    extends RDD[((Int, Int, Long))](prev) with Lineage[(Int, Int, Long)] {

  override def ttag = classTag[(Int, Int, Long)]

  override def lineageContext = prev.lineageContext

  override def getPartitions: Array[Partition] = firstParent[((Int, Int, Long), Any)].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[((Int, Int, Long), Any)].iterator(split, context).map(r => r._1)

  override def collect(): Array[(Int, Int, Long)] = {
    val results = prev.context.runJob(
      prev.map(r => r._1), (iter: Iterator[(Int, Int, Long)]) => iter.toArray.distinct
    )
    Array.concat(results: _*)
  }

  override def filter(f: ((Int, Int, Long)) => Boolean): LineageRDD = {
    new LineageRDD(firstParent[((Int, Int, Long), Any)].filter(r => f(r._1)).cache())
  }

  def goNext(): LineageRDD = {
    val next = prev.lineageContext.getForward
    var shuffled: Lineage[((Int, Int, Long), Any)] = prev
    if(next.isInstanceOf[TapPreShuffleLRDD[_]]) {
      val part = new LocalityAwarePartitioner(next.partitions.size)
      shuffled = new ShuffledLRDD[(Int, Int, Long), Any, Any](prev, part)
    }
    new LineageRDD(
      rightJoinLeft(shuffled, next)
        .map(r => (r._2, r._1))
        .asInstanceOf[Lineage[((Int, Int, Long), Any)]]
        .cache()
    )
  }

  def goBack(): LineageRDD = {
    val next = prev.lineageContext.getBackward
    if (next.isDefined) {
        var shuffled: Lineage[((Int, Int, Long), Any)] = prev
      if(next.get.isInstanceOf[TapPreShuffleLRDD[_]]) {
        val part = new LocalityAwarePartitioner(next.get.partitions.size)
        shuffled = new ShuffledLRDD[(Int, Int, Long), Any, Any](prev, part)
      }

      new LineageRDD(
        rightJoinLeft(shuffled, next.get)
          .map(r => (r._2, r._1))
          .asInstanceOf[Lineage[((Int, Int, Long), Any)]])
        .cache()
    } else {
      new LineageRDD(
        prev.asInstanceOf[Lineage[(Any, (Int, Int, Long))]].map(r => (r._2, r._1)).cache()
      )
    }
  }

  private[spark] def go(times: Int, direction: Direction = Direction.FORWARD): LineageRDD = {
    var result = this
    var counter = 0
    try {
      while(counter < times) {
        if(direction == Direction.BACKWARD) {
          result = result.goBack
        } else {
          result = result.goNext
        }
        counter = counter + 1
      }
    } catch {
      case e: UnsupportedOperationException =>
    } finally {
      if(result == this) {
        throw new UnsupportedOperationException
      }
    }
    // Never reach this but otherwise will not compile
    result
  }

  def goBack(times: Int = 1) = go(times, Direction.BACKWARD)

  def goNext(times: Int = 1) = go(times)

  def goBackAll() = go(Int.MaxValue, Direction.BACKWARD)

  def goNextAll() = go(Int.MaxValue)

  def show(): ShowRDD = {
    val position = prev.lineageContext.getCurrentLineagePosition
    if(position.isDefined) {
      var result: ShowRDD = null
      if (position.get.isInstanceOf[TapHadoopLRDD[_, _]]) {
        result = new ShowRDD (prev.zipPartitions(position.get.asInstanceOf[Lineage[((Int, Int, Long), (String, Long))]],
          position.get.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
            .map(r=> (r._1.get(), r._2.toString))) {
          (buildIter, streamIter1, streamIter2) =>
            val hashSet = new java.util.HashSet[(Int, Int, Long)]()
            val hashMap = new java.util.HashMap[Long, CompactBuffer[(Int, Int, Long)]]()
            var rowKey: (Int, Int, Long) = null

            // Create a Hash set of buildKeys
            while (buildIter.hasNext) {
              rowKey = buildIter.next()._1
              val keyExists = hashSet.contains(rowKey)
              if (!keyExists) {
                hashSet.add(rowKey)
              }
            }

            while(streamIter1.hasNext) {
              val current = streamIter1.next()
              if(hashSet.contains(current._1)) {
                var values = hashMap.get(current._2)
                if(values == null) {
                  values = new CompactBuffer[(Int, Int, Long)]()
                }
                values += current._1
                hashMap.put(current._2._2, values)
              }
            }
            streamIter2.flatMap(current => {
              val values = if(hashMap.get(current._1) != null) {
                hashMap.get(current._1)
              } else {
                new CompactBuffer[(Int, Int, Long)]()
              }
              values.map(record => (record, current._2))
            })
        }.cache())
      } else if(position.get.isInstanceOf[TapPreShuffleLRDD[_]]) {
        result = new ShowRDD (
          rightJoinLeft(prev.asInstanceOf[Lineage[((Int, Int, Long), Any)]], position.get.asInstanceOf[TapPreShuffleLRDD[_]]
            .getCachedData
            .asInstanceOf[Lineage[(Any, (Any, (Int, Int, Long)))]]
            .map(r => (r._2._2, ((r._1, r._2._1), r._2._2._3).toString())))
          .asInstanceOf[Lineage[((Int, Int, Long), String)]]
          .cache()
        )
      } else if(position.get.isInstanceOf[TapPostShuffleLRDD[_]]) {
        result = new ShowRDD(
          rightJoinLeft(prev.asInstanceOf[Lineage[((Int, Int, Long), Any)]], position.get.asInstanceOf[TapPostShuffleLRDD[_]]
            .getCachedData.setCaptureLineage(false)
            .asInstanceOf[Lineage[((Any, Any), (Int, Int, Long))]]
            .map(r => (r._2, ((r._1._1, r._1._2), r._2._3).toString())))
          .asInstanceOf[Lineage[((Int, Int, Long), String)]]
          .cache()
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
