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
import org.apache.spark.lineage.{Direction, LocalityAwarePartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.lineage.LineageContext._

import scala.reflect._

private[spark]
class LineageRDD(prev: Lineage[(RecordId, Any)])
extends RDD[(RecordId)](prev) with Lineage[RecordId]
{
  override def ttag = classTag[RecordId]

  override def lineageContext = prev.lineageContext

  override def getPartitions: Array[Partition] = firstParent[(RecordId, Any)].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[(RecordId, Any)].iterator(split, context).map(r => r._1)

  override def collect(): Array[RecordId] =
  {
    val results = prev.context.runJob(
      prev.map(r => r._1), (iter: Iterator[RecordId]) => iter.toArray.distinct
    )
    Array.concat(results: _*)
  }

  override def filter(f: (RecordId) => Boolean): LineageRDD =
  {
    new LineageRDD(firstParent[(RecordId, Any)].filter(r => f(r._1)).cache())
  }

  def dump: RDD[(RecordId, Any)] = prev

  def goNext(): LineageRDD =
  {
    val next = prev.lineageContext.getForward
    var shuffled: Lineage[(RecordId, Any)] = prev.lineageContext.getCurrentLineagePosition.get match {
      case _: TapPostShuffleLRDD[_] =>
        val part = new LocalityAwarePartitioner(next.partitions.size)
        new ShuffledLRDD[RecordId, Any, Any](prev, part).setMapSideCombine(false)
      case _ => prev
    }

    if(prev.lineageContext.getCurrentLineagePosition.get.isInstanceOf[TapPostShuffleLRDD[_]]) {
      new LineageRDD(
        rightJoin(shuffled, next)
          .map(r => (r._2, r._1))
          .asInstanceOf[Lineage[(RecordId, Any)]]
          .cache()
      )
    } else {
      new LineageRDD(
        rightJoinShort(shuffled.map(r => ((r._1._2, r._1._3), r._2)), next.asInstanceOf[Lineage[((Short, Int), Any)]])
          .map(r => (r._2, r._1))
          .asInstanceOf[Lineage[(RecordId, Any)]]
          .cache()
      )
    }
  }

  def goBack(path: Int = 0): LineageRDD =
  {
    val next = prev.lineageContext.getBackward(path)
    if (next.isDefined) {
      val shuffled: Lineage[(RecordId, Any)] = next.get match {
        case _: TapPreShuffleLRDD[_] | _: TapCoGroupLRDD[_] | _: FilteredLRDD[_] =>
          val part = new LocalityAwarePartitioner(next.get.partitions.size)
          new ShuffledLRDD[RecordId, Any, Any](prev, part).setMapSideCombine(false)
        case _ => prev
      }

      if (next.get.isInstanceOf[TapPreShuffleLRDD[_]]) {
        new LineageRDD(
          rightJoin(shuffled, next.get)
            .map(r => ((0: Short, r._2.asInstanceOf[(Short, Int)]._1, r._2.asInstanceOf[(Short, Int)]._2), r._1))
            .asInstanceOf[Lineage[(RecordId, Any)]])
          .cache()
      } else {
        new LineageRDD(
          rightJoin(shuffled, next.get)
            .map(r => (r._2, r._1))
            .asInstanceOf[Lineage[(RecordId, Any)]])
          .cache()
      }
    } else {
      val previous = lineageContext.getCurrentLineagePosition.get match {
        case _: TapCoGroupLRDD[_] =>
          val filter = lineageContext.getCurrentLineagePosition.get.id
          prev.filter(r => r._2.asInstanceOf[(RecordId)]._1.equals(filter))
        case _ => prev
      }
      new LineageRDD(
        previous.map(r => (r._2, r._1)).cache()
      )
    }
  }

  def goBackAll(times: Int = Int.MaxValue) = go(times, Direction.BACKWARD)

  def goNextAll(times: Int = Int.MaxValue) = go(times)

//  def dumpTrace: RDD[_] = {
//    // TODO works only for WordCount
//    import org.apache.spark.SparkContext._
//    var show = this.show().dump
//    dump.collect().foreach(println)
//    show.collect().foreach(println)
//    var result1 = show.map(r => ((r._1._2, r._1._3), r._2)).join(dump.map(r => ((r._1._2, r._1._3), r._2.asInstanceOf[(Short, Short, Int)]))).map(r => (r._2._2, (r._1, r._2._1))).cache()
//    result1.collect().foreach(println)
//
//    lineageContext.getBackward(0)
//    val next = lineageContext.getBackward(0)
//      val shuffled: RDD[(RecordId, Any)] = next.get match {
//        case _: TapPreShuffleLRDD[_] | _: TapCoGroupLRDD[_] | _: FilteredLRDD[_] =>
//          val part = new LocalityAwarePartitioner(next.get.partitions.size)
//          new ShuffledLRDD[RecordId, Any, Any](result1, part).setMapSideCombine(false)
//        case _ => result1
//      }
//
//      if (next.get.isInstanceOf[TapPreShuffleLRDD[_]]) {
//          shuffled.join(next.get)
//            .map(r => ((0: Short, r._2.asInstanceOf[(Short, Int)]._1, r._2.asInstanceOf[(Short, Int)]._2), r._1))
//            .asInstanceOf[Lineage[(RecordId, Any)]]
//          .cache()
//      }
//
//    var tmp = lineage.dump
//    tmp.collect().foreach(println)
//    //var result2 = result1.join(tmp).map(r => (r._2._2, (r._1, r._2._1)))
//    //result2.collect().foreach(println)
//    //result2
//    result1
//  }

  def show(): ShowRDD =
  {
    val position = prev.lineageContext.getCurrentLineagePosition
    if(position.isDefined) {
      var result: ShowRDD = null
      position.get match {
        case _: TapHadoopLRDD[_, _] =>
          result = new ShowRDD(
            join3Way(
              prev.map(r => ((r._1._2, r._1._3), r._2)),
              position.get.asInstanceOf[Lineage[((Short, Int), (String, Long))]],
              position.get.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
                .map(r=> (r._1.get(), r._2.toString))
            ).map(r => ((0:Short, r._1._1, r._1._2), r._2)).cache() // Added dummy id. To be removed
          )
        case _: TapParallelCollectionLRDD[_] =>
          result = new ShowRDD(
              position.get.asInstanceOf[Lineage[(RecordId, Any)]]
                .map(r=> (r._1, (r._2, r._1._3).toString)).cache()
            )
        case _: TapPreCoGroupLRDD[_] =>
          val part = new LocalityAwarePartitioner(prev.partitions.size)
          val left = rightJoin(
            new ShuffledLRDD[RecordId, Any, Any](prev, part).setMapSideCombine(false),
            position.get)
          val right = position.get.getCachedData.setCaptureLineage(true)
            .asInstanceOf[CoGroupedLRDD[_]].map {
              case (v, Array(vs, w1s)) =>
                ((v, vs.asInstanceOf[Iterable[(_, (_, RecordId))]]),
                  (v, w1s.asInstanceOf[Iterable[(_, RecordId)]]))
            }.flatMap(
              r => for(v <- r.productIterator) yield(v.asInstanceOf[(_, Iterable[(_, RecordId)])])
            ).flatMap( r => for(v <- r._2) yield(v._2, ((r._1, v._1), v._2._3).toString()))

          result = new ShowRDD(rightJoin(
              left, new ShuffledLRDD[RecordId, Any, Any](right, part).setMapSideCombine(false)
            ).cache())
        case _: TapPreShuffleLRDD[_] =>
          result = new ShowRDD(rightJoin(
              prev,
              position.get.asInstanceOf[TapPreShuffleLRDD[_]]
                .getCachedData
                .asInstanceOf[Lineage[(Any, (Any, RecordId))]]
                .map(r => (r._2._2, ((r._1, r._2._1), r._2._2._3).toString()))
            ).cache()
          )
        case _: TapPostShuffleLRDD[_] =>
          result = new ShowRDD(rightJoinShort(
              prev.map(r => ((r._1._2, r._1._3), r._2)),
              position.get.asInstanceOf[TapPostShuffleLRDD[_]]
                .getCachedData.setCaptureLineage(false)
                .asInstanceOf[Lineage[((Any, Any), (Short, Int))]]
                .map(r => (r._2, ((r._1._1, r._1._2), r._2._2).toString()))
            ).map(r => ((0:Short, r._1._1, r._1._2), r._2)).cache() // Added dummy id. To be removed
          )
        case _: TapCoGroupLRDD[_] =>
          val part = new LocalityAwarePartitioner(
            position.get.getCachedData.setCaptureLineage(true).partitions.size)
          val left = rightJoin(
            prev,
            position.get
          ).map(r => (r._2, r._1))
          val right = new ShuffledLRDD[RecordId, Any, Any](
            position.get.getCachedData.setCaptureLineage(true).map {
              case (v, Array(vs, w1s)) => (
                (v, vs.asInstanceOf[Iterable[(_, (_, RecordId))]]),
                (v, w1s.asInstanceOf[Iterable[(_, RecordId)]])
                )
            }.flatMap(
                r => for(v <- r.productIterator) yield(v.asInstanceOf[(_, Iterable[(_, RecordId)])])
              ).flatMap( r => for(v <- r._2) yield(v._2, ((r._1, v._1), v._2._3).toString())), part).setMapSideCombine(false)

          result = new ShowRDD(rightJoin(
            new ShuffledLRDD[RecordId, Any, Any](left, part).setMapSideCombine(false), right).cache()
          )
        case _ => throw new UnsupportedOperationException("what cache are you talking about?")
      }

      result.collect.foreach(println)
      result
    } else {
      throw new UnsupportedOperationException("what position are you talking about?")
    }
  }

  private[spark] def go(times: Int, direction: Direction = Direction.FORWARD): LineageRDD =
  {
    var result = this
    var counter = 0
    try {
      while(counter < times) {
        if(direction == Direction.BACKWARD) {
          result = result.goBack()
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
}
