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

import java.util

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark._
import org.apache.spark.lineage.Direction.Direction
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.{Direction, LocalityAwarePartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.collection.CompactBuffer
import org.roaringbitmap.RoaringBitmap

import scala.reflect._

private[spark]
class LineageRDD(val prev: Lineage[(Any, Any)])
extends RDD[Any](prev) with Lineage[Any]
{
  override def ttag = classTag[Any]

  override def lineageContext = prev.lineageContext

  override def getPartitions: Array[Partition] = firstParent[(Any, Any)].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[(Any, Any)].iterator(split, context).map(r => r._1)

  var prevResult = Array[(Int, (Any, Any))]()

  override def collect(): Array[Any] =
  {
    val result = prev.context.runJob(
      prev, (iter: Iterator[(Any, Any)]) => iter.toArray.distinct
    )
    prevResult = Array.concat(result: _*).zipWithIndex.map(r => (r._2, r._1))
    prevResult.map(r => (r._1, r._2._1))
  }

  override def filter(f: (Any) => Boolean): LineageRDD =
  {
    new LineageRDD(firstParent[(Any, Any)].filter(r => f(r._1)).cache())
  }

  def filter(f: Int): LineageRDD =
  {
    val values = prevResult.filter(r => r._1 == f).map(_._2)
    new LineageRDD(firstParent[(Any, Any)].filter(r => values.contains(r)).cache())
  }

  def dump: RDD[(Any, Any)] = prev

  def goNext(): LineageRDD =
  {
    val next = prev.lineageContext.getForward
    val shuffled: Lineage[(_, _)] = prev.lineageContext.getCurrentLineagePosition.get match {
      case _: TapPostShuffleLRDD[_] =>
        val part = new HashPartitioner(next.partitions.size)
        new ShuffledLRDD[Any, Any, Any](prev.asInstanceOf[Lineage[((Long, Int), Any)]].map(r => (r._1._2, r._2)), part).setMapSideCombine(false)
      case _ => prev
    }

    prev.lineageContext.getCurrentLineagePosition.get match {
      case _: TapPostShuffleLRDD[_] =>
        new LineageRDD(
          rightJoinSuperShort(shuffled.asInstanceOf[Lineage[(Int, Any)]], next.asInstanceOf[Lineage[((CompactBuffer[Long], Int), Any)]].map(r => (r._1._2, r._2)))
            .map(r => ((0L, r._2), r._1))
            .asInstanceOf[Lineage[(Any, Any)]]
            .cache()
        )
      case _: TapLRDD[_] =>
        new LineageRDD(
          rightJoinShort(shuffled.map(r => r.asInstanceOf[((Int, Int), Any)]), next)
            .map(r => (r._2, r._1))
            .asInstanceOf[Lineage[(Any, Any)]]
            .cache()
        )
      case _ => // Check this
        new LineageRDD(
          rightJoinShort(shuffled.map(r => ((r._1.asInstanceOf[(Int, Int)]._1, r._1.asInstanceOf[(Int, Int)]._2), r._2)), next)
            .map(r => (r._2, r._1))
            .asInstanceOf[Lineage[(Any, Any)]]
            .cache()
        )
    }
  }

  def goBack(path: Int = 0): LineageRDD =
  {
    val next = prev.lineageContext.getBackward(path)
    if (next.isDefined) {
      val shuffled: Lineage[(Any, Any)] = next.get match {
        case _: TapPreShuffleLRDD[_] | _: TapCoGroupLRDD[_] | _: FilteredLRDD[_] =>
          val part = new LocalityAwarePartitioner(next.get.partitions.size)
          new ShuffledLRDD[Any, Any, Any](prev, part).setMapSideCombine(false)
        case _ => prev
      }

      if (next.get.isInstanceOf[TapPreShuffleLRDD[_]]) {
        new LineageRDD(
          rightJoinNew(shuffled.asInstanceOf[Lineage[((Long, Int), Any)]], next.get.asInstanceOf[Lineage[((Long, Int), Any)]])
            .flatMap(r => r._2.asInstanceOf[RoaringBitmap].toArray.map(r2 => ((r._1._1, r2), r._1)))
            .asInstanceOf[Lineage[(Any, Any)]])
          .cache()
      } else {
        new LineageRDD(rightJoin(shuffled, next.get)
          .map(r => (r._2, r._1))
          .asInstanceOf[Lineage[(Any, Any)]]).cache()
      }
    } else {
      val previous = lineageContext.getCurrentLineagePosition.get match {
        case _: TapCoGroupLRDD[_] =>
          val filter = lineageContext.getCurrentLineagePosition.get.id
          prev.filter(r => r._2.asInstanceOf[(RecordId)]._1.equals(filter))
        case _: TapPreShuffleLRDD[_] =>
          prev.asInstanceOf[Lineage[(Any, (util.ArrayDeque[Long], Int))]].flatMap(r1 => r1._2._1.toArray.map(r2 => (r1._1, (r2, r1._2._2))))
        case _ => prev
      }
      new LineageRDD(
        previous.map(r => (r._2, r._1)).cache()
      )
    }
  }

  def goBackAll(times: Int = Int.MaxValue) = go(times, Direction.BACKWARD)

  def goNextAll(times: Int = Int.MaxValue) = go(times)

//  def dumpFullTrace: RDD[_] = {
//    // TODO works only for WordCount
//    import org.apache.spark.SparkContext._
//    var show = this.showForDump
//    var result1 = show.join(dump.map(r => ((r._1._2, r._1._3), r._2.asInstanceOf[(Short, Short, Int)]))).map(r => (r._2._2, (r._1, r._2._1))).cache()
//
//    lineageContext.getBackward(0)
//    val next = lineageContext.getBackward(0)
//          val part = new LocalityAwarePartitioner(next.get.partitions.size)
//          val shuffled = new ShuffledRDD[RecordId, Any, Any](result1, part).setMapSideCombine(false)
//
//          val result2 = next.get.join(shuffled).map(r => ((r._2._1).asInstanceOf[(Short, Int)], (r._1, r._2._2)))
//          .cache()
//
//    val position = lineageContext.getCurrentLineagePosition
//    val result3 = join3Way(
//      result2.asInstanceOf[Lineage[((Short, Int), Any)]],
//      position.get.asInstanceOf[Lineage[((Short, Int), (String, Long))]],
//      position.get.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
//        .map(r => (r._1.get(), r._2.toString))
//    ).cache()
//    //var result2 = result1.join(tmp).map(r => (r._2._2, (r._1, r._2._1)))
//    val result4 = result3.join(result2).map(r => ((r._2._1), (r._1, r._2._2)))
//    //result2
//    result4
//  }

//  def showForDump(): RDD[((Short, Int), String)] = {
//    val position = prev.lineageContext.getCurrentLineagePosition
//      val result: RDD[((Short, Int), String)] = rightJoinShort(
//            prev.map(r => ((r._1.asInstanceOf[(Int, Int)]._1, r._1.asInstanceOf[(Int, Int)]._2), r._2)),
//            position.get.asInstanceOf[TapPostShuffleLRDD[_]]
//              .getCachedData.setCaptureLineage(false)
//              .asInstanceOf[Lineage[((Any, Any), (Int, Int))]]
//              .map(r => (r._2, (r._1._1, r._1._2).toString()))
//          ).asInstanceOf[RDD[((Short, Int), String)]].cache() // Added dummy id. To be removed
//      result
//  }

//  def dumpTrace: Unit = {
//    val current = lineageContext.getCurrentLineagePosition
//    var position = prev.lineageContext.getCurrentLineagePosition
//    rightJoinShort(
//      prev.map(r => ((r._1._2, r._1._3), r._2)),
//      position.get.asInstanceOf[TapPostShuffleLRDD[_]]
//        .getCachedData.setCaptureLineage(false)
//        .asInstanceOf[Lineage[((Any, Any), (Short, Int))]]
//        .map(r => (r._2, ((r._1._1, r._1._2)).toString()))
//    ).saveAsTextFile("dump/output")
//
//    prev.asInstanceOf[RDD[((Short, Short, Int), (Short, Short, Int))]].map(r => ((r._1._2, r._1._3), (r._2._2, r._2._3))).saveAsTextFile("dump/join1")
//    lineageContext.getBackward()
//    val next = lineageContext.getBackward().get.map(r => ((r._1._2, r._1._3), r._2))
//    next.saveAsTextFile("dump/join2")
//    position = lineageContext.getCurrentLineagePosition
//    join3Way(
//      next.asInstanceOf[Lineage[((Short, Int), Any)]],
//      position.get.asInstanceOf[Lineage[((Short, Int), (String, Long))]],
//      position.get.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
//        .map(r => (r._1.get(), r._2.toString))).saveAsTextFile("dump/input")
//    lineageContext.setCurrentLineagePosition(current)
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
              prev.map(r => (r._1)).asInstanceOf[Lineage[(Long, Int)]],
              position.get.asInstanceOf[Lineage[(Long, Int)]],
              position.get.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
                .map(r=> (r._1.get(), r._2.toString))
            ).map(r => ((0, r._1), r._2)).cache() // Added dummy id. To be removed
          )
        case _: TapParallelCollectionLRDD[_] =>
          result = new ShowRDD(
              position.get.asInstanceOf[Lineage[(RecordId, Any)]]
                .map(r=> (r._1, (r._2, r._1._2).toString)).cache()
            )
        case _: TapPreCoGroupLRDD[_] =>
          val part = new LocalityAwarePartitioner(prev.partitions.size)
          val left = rightJoin(
            new ShuffledLRDD[Any, Any, Any](prev, part).setMapSideCombine(false),
            position.get)
          val right = position.get.getCachedData.setCaptureLineage(true)
            .asInstanceOf[CoGroupedLRDD[_]].map {
              case (v, Array(vs, w1s)) =>
                ((v, vs.asInstanceOf[Iterable[(_, (_, RecordId))]]),
                  (v, w1s.asInstanceOf[Iterable[(_, RecordId)]]))
            }.flatMap(
              r => for(v <- r.productIterator) yield(v.asInstanceOf[(_, Iterable[(_, RecordId)])])
            ).flatMap( r => for(v <- r._2) yield(v._2, ((r._1, v._1), v._2._2).toString()))

          result = new ShowRDD(rightJoin(
              left, new ShuffledLRDD[RecordId, Any, Any](right, part).setMapSideCombine(false)
            ).cache())
        case _: TapPreShuffleLRDD[_] =>
          val current = if(!prev.lineageContext.getlastOperation.isDefined) {
            prev.asInstanceOf[Lineage[((Long, Int), Any)]]
          } else {
            prev.lineageContext.getlastOperation.get match {
              case Direction.FORWARD =>
                val part = new HashPartitioner(prev.partitions.size)
                new ShuffledLRDD[Any, Any, Any](prev.asInstanceOf[Lineage[((Long, Int), Any)]].map(r => (r._1._2, r)), part).setMapSideCombine(false)
                  .map(r => r._2).asInstanceOf[Lineage[((Long, Int), Any)]]
              case Direction.BACKWARD => prev.asInstanceOf[Lineage[((Long, Int), Any)]]
            }
          }

          result = new ShowRDD(rightJoinNew(
            current,
            position.get.asInstanceOf[TapPreShuffleLRDD[_]]
              .getCachedData
              .asInstanceOf[Lineage[(Any, (Any, Long))]]
              .map { r =>
              val hash = r._1.hashCode()
              ((r._2._2, hash), ((r._1, r._2._1), hash).toString())
            }
          ).cache().asInstanceOf[Lineage[((Int, Int), Any)]]
          )
        case _: TapPostShuffleLRDD[_] =>
          val current = if(!prev.lineageContext.getlastOperation.isDefined) {
            prev.map(r => (r._2.asInstanceOf[(CompactBuffer[Long], Int)]._2, r._1))
          } else {
            val tmp = prev.lineageContext.getCurrentLineagePosition.get match {
              case _: TapPreShuffleLRDD[_] | _: TapPreCoGroupLRDD[_] | _: TapPostShuffleLRDD[_] => prev
              case _: TapLRDD[_] => rightJoinSuperShort(prev.asInstanceOf[Lineage[((Int, Int), Any)]].map(r => (r._1._2, r._2)), position.get.asInstanceOf[Lineage[(Int, Any)]])
            }
            tmp.lineageContext.getlastOperation.get match {
              case Direction.FORWARD => tmp.map(r => (r._2, r._1))
              case _ => tmp.map(r => (r._2.asInstanceOf[(CompactBuffer[Long], Int)]._2, r._1))
            }
          }
          result = new ShowRDD(rightJoinSuperShort(
            current.asInstanceOf[Lineage[(Int, Any)]],
              position.get.asInstanceOf[TapPostShuffleLRDD[_]]
                .getCachedData.setCaptureLineage(false)
                .asInstanceOf[Lineage[(String, Int)]]
                .map(r => (r._1.hashCode, r.toString()))
            ).map(r => ((0, r._1), r._2)).cache() // Added dummy id. To be removed
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
              ).flatMap( r => for(v <- r._2) yield(v._2, ((r._1, v._1), v._2._2).toString())), part).setMapSideCombine(false)

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
