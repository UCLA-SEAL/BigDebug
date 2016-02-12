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

import java.io.File
import java.sql.DriverManager

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileUtil, FileSystem}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark._
import org.apache.spark.lineage.rdd.Lineage._
import org.apache.spark.lineage.Direction.Direction
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.{Direction, HashAwarePartitioner, LocalityAwarePartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.PackIntIntoLong
import org.apache.spark.util.collection.CompactBuffer

import scala.reflect._

private[spark]
class LineageRDD(val prev: Lineage[(RecordId, Any)]) extends RDD[Any](prev) with Lineage[Any] {

  def this(prev: => Lineage[(Int, Any)]) = this(prev.map(r => ((Dummy, r._1), r._2)))

  override def lineageContext = prev.lineageContext

  override def ttag = classTag[Any]

  override def getPartitions: Array[Partition] = firstParent[(Any, Any)].partitions

  override def compute(split: Partition, context: TaskContext) = {
    firstParent[(Any, Any)].iterator(split, context).map(r => r._1)
  }

  var prevResult = Array[(Int, (Any, Any))]()

  override def collect(): Array[Any] = {
    val result = prev.context.runJob(
      prev, (iter: Iterator[(Any, Any)]) => iter.toArray.distinct
    ).filter(_ != null) // Needed for removing results from empty partitions

    Array.concat(result: _*).asInstanceOf[Array[Any]]
//    prevResult = Array.concat(result: _*).zipWithIndex.map(_.swap)
//    prevResult.map(r => (r._1, r._2._1))
  }

  override def filter(f: (Any) => Boolean): LineageRDD = {
    new LineageRDD(firstParent[(Any, Any)].filter(r => f(r._1)).cache())
  }

  def filter(f: Int): LineageRDD = {
    if(!prevResult.isEmpty) {
      val values = prevResult.filter(r => r._1 == f).map(_._2)
      firstParent[(RecordId, Any)].filter(r => values.contains(r)).cache()
    } else {
      new LineageRDD(firstParent[(Int, Any)].filter(r => r._1 == f).cache())
      //new LineageRDD(firstParent[((Long, Int), Any)].filter(r => r._1._2 == f).cache())
    }
  }

  override def saveAsDBTable(url: String, username: String, password: String, path: String, driver: String): Unit = {
    var f: Iterator[(Any, Any)] => Unit = null
    lineageContext.getCurrentLineagePosition.get match {
      case post: TapPostShuffleLRDD[_] =>
        f = (it: Iterator[(Any, Any)]) => {
          Class.forName(driver)
          val conn = DriverManager.getConnection(url,username,password)
          var statement = "INSERT INTO " + path + " (input,output) VALUES "
          var count = 0
          for (output <-it.asInstanceOf[Iterator[((Int, Int), (CompactBuffer[Int], Int))]])
          {
            output._2._1.foreach( id => {
              statement +=
                ("(" + id + output._2._2.toString + "," + output._1._1 + output._1._2 + "), ")
              count += 1
              if(count == 1000) {
                val del = conn.prepareStatement(statement.dropRight(2))
                del.executeUpdate()
                statement = "INSERT INTO " + path + " (input,output) VALUES "
                count = 0
              }
            })
          }
          val del = conn.prepareStatement (statement.dropRight(2))
          del.executeUpdate()
          conn.close()
        }
      case pre: TapPreShuffleLRDD[_] =>
        f = (it: Iterator[(Any, Any)]) => {
          Class.forName(driver)
          val conn = DriverManager.getConnection(url,username,password)
          var statement = "INSERT INTO " + path + " (input,output) VALUES "
          var count = 0
          for (output <-it.asInstanceOf[Iterator[(RecordId, Array[Int])]])
          {
            output._2.foreach( id => {
              statement += ("(" + id + output._1._1 + "," + output._1._1 + output._1._2 + "), ")
              count += 1
              if(count == 1000) {
                val del = conn.prepareStatement(statement.dropRight(2))
                del.executeUpdate()
                statement = "INSERT INTO " + path + " (input,output) VALUES "
                count = 0
              }
            })
          }
          val del = conn.prepareStatement (statement.dropRight(2))
          del.executeUpdate()
          conn.close()
        }
      case hadoop: TapHadoopLRDD[_, _] =>
        f = (it: Iterator[(Any, Any)]) => {
          Class.forName(driver)
          val conn = DriverManager.getConnection(url,username,password)
          var statement = "INSERT INTO " + path + " (input,output) VALUES "
          var count = 0
          for (output <-it.asInstanceOf[Iterator[RecordId]])
          {
            statement += ("(" + output._1 + "," + output._2 + "), ")
            count += 1
            if(count == 1000) {
              val del = conn.prepareStatement(statement.dropRight(2))
              del.executeUpdate()
              statement = "INSERT INTO " + path + " (input,output) VALUES "
              count = 0
            }
          }
          val del = conn.prepareStatement (statement.dropRight(2))
          del.executeUpdate()
          conn.close()
        }
      case _ =>
        f = (it: Iterator[(Any, Any)]) => {
          Class.forName(driver)
          val conn= DriverManager.getConnection(url,username,password)
          var statement = "INSERT INTO " + path + " (input,output) VALUES "
          var count = 0
          for (output <-it.asInstanceOf[Iterator[(RecordId,RecordId)]])
          {
            statement += ("(" + output._2._2 + "," + output._1._2 + "), ")
            count += 1
            if(count == 1000) {
              val del = conn.prepareStatement(statement.dropRight(2))
              del.executeUpdate()
              statement = "INSERT INTO " + path + " (input,output) VALUES "
              count = 0
            }
          }
          val del = conn.prepareStatement(statement.dropRight(2))
          del.executeUpdate()
          conn.close()
        }
    }

    val cleanF = prev.context.clean(f)
    prev.context.runJob(prev, (iter: Iterator[(Any, Any)]) => cleanF(iter))
  }

  override def saveAsCSVFile(path: String): Unit = {
    def merge(srcPath: String, dstPath: String): Unit =  {
      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)
      FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
    }

    val file = path + "_part"
    FileUtil.fullyDelete(new File(file))

    FileUtil.fullyDelete(new File(path))

    val result = lineageContext.getCurrentLineagePosition.get match {
      case post: TapPostShuffleLRDD[_] =>
        post.mapPartitions((it: Iterator[_]) => {
          it.asInstanceOf[Iterator[((Int, Short), (CompactBuffer[Int], Int))]].flatMap(output =>
            output._2._1.map(id => {
              "" + id + output._2._2 + "," + PackIntIntoLong(output._1._1,output._1._2)}
              ))})
      case pre: TapPreShuffleLRDD[_] =>
        pre.mapPartitions((it: Iterator[_]) => {
          it.asInstanceOf[Iterator[((Long, Int), Array[Int])]].flatMap(output =>
            output._2.map(id => {
              "" + PackIntIntoLong(id, PackIntIntoLong.getRight(output._1._1)) + ","
              + output._1._1 + output._1._2 }
            ))})
      case hadoop: TapHadoopLRDD[_, _] =>
        hadoop.mapPartitions((it: Iterator[_]) => {
          it.asInstanceOf[Iterator[(Long, Long)]].map(output =>
            "" + output._1.toString + "," + output._2.toString + ""
          )})
      case tap =>
        tap.mapPartitions((it: Iterator[_]) => {
          it.asInstanceOf[Iterator[(Long, Long)]].map(output =>
            "" + output._2.toString + "," + output._1.toString + ""
          )})
    }

    result.saveAsTextFile(file)

    merge(file, path)
  }

  def getBack(): LineageRDD = {
    prev.lineageContext.getBackward()
    prev.lineageContext.getCurrentLineagePosition.get
  }

  def goNext(): LineageRDD = {
    val next = prev.lineageContext.getForward
    lineageContext.getCurrentLineagePosition.get match {
      case post: TapPostShuffleLRDD[_] =>
        val part = new HashAwarePartitioner(next.partitions.size)
        val shuffled = new ShuffledLRDD[Int, Any, Any](prev.asInstanceOf[Lineage[(Int, Any)]], part)
        rightJoin[Int, Any](shuffled, next).map(r => ((Dummy, r._2), r._1)).cache()
      case _ => rightJoin[Int, Any](prev, next).map(_.swap).cache()
    }
  }

  def goBack(path: Int = 0): LineageRDD = {
    val next = lineageContext.getBackward(path)
    if (next.isDefined) {
      val shuffled: Lineage[(RecordId, Any)] = lineageContext.getCurrentLineagePosition.get match {
        case _: TapPreShuffleLRDD[_]  =>
          val part = new LocalityAwarePartitioner(next.get.partitions.size)
            new ShuffledLRDD[RecordId, Any, Any](
              rightJoin[Int, (CompactBuffer[Int], Int)](prev, lineageContext.getLastLineageSeen.get)
                .map(r => (r._2, r._1))
                .flatMap(r => r._1._1.map(r2 => ((r2, r._1._2), (Dummy, r._2)))), part)
        case _ => prev
      }

      lineageContext.getCurrentLineagePosition.get match {
        case _: TapPreShuffleLRDD[_] =>
          rightJoin(shuffled, next.get)
            .flatMap(r => r._2.asInstanceOf[Array[Int]].map(b => (r._1, (r._1._1, b))))
            .cache()
        case _: TapParallelCollectionLRDD[_] =>
          rightJoin[Int, Any](shuffled, next.get.map(_.swap).asInstanceOf[Lineage[(Int, Any)]])
          .map(_.swap).cache()
        case _: TapPostCoGroupLRDD[_] =>
          rightJoin[Int, (CompactBuffer[Int], Int)](shuffled.map(_.swap),next.get)
            .flatMap(r => r._2._1.map(c => ((c, r._1), (c, r._2._2))))
            .cache()
        case _ =>
          rightJoin[Int, Any](shuffled.map(_.swap), next.get.map { r =>
              if(r._1.isInstanceOf[Tuple2[_, _]])(r._1._2, r._2) else r.asInstanceOf[(Int, Any)]})
            .map(r => ((Dummy, r._1), r._2))
            .cache()
      }
    } else {
      lineageContext.getCurrentLineagePosition.get match {
        case _: TapPreShuffleLRDD[_] =>
          val part = new LocalityAwarePartitioner(prev.partitions.size)
          val shuffled = new ShuffledLRDD[RecordId, Any, Any](prev
            .asInstanceOf[Lineage[(Any, (CompactBuffer[Int], Int))]]
            .flatMap(r => r._2._1.map(c => ((c, r._2._2), (Dummy, r._1)))), part)
          rightJoin[RecordId, Array[Int]](shuffled, lineageContext.getCurrentLineagePosition.get)
            .flatMap(r => r._2.map(b => (r._1, (Dummy, b))))
            .cache
        case _ => prev.map(_.swap).cache()
      }
    }
  }

  def goBackAll(times: Int = Int.MaxValue) = go(times, Direction.BACKWARD)

  def goNextAll(times: Int = Int.MaxValue) = go(times)

  def show(): ShowRDD[_] = {
    val position = lineageContext.getCurrentLineagePosition
    if(position.isDefined) {
      var result: ShowRDD[_] = null
      position.get match {
        case _: TapHadoopLRDD[_, _] => result = new ShowRDD[Long](
              rightJoin[Long, String](prev.map(_.swap).asInstanceOf[Lineage[(Any, (RecordId))]],
                position.get.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
                  .map(r=> (r._1.get(), r._2.toString))
              ).cache())
//            case _ =>
//              prev.collect().foreach(println)
//              result = new ShowRDD(
//                            join3Way(
//                              prev.map(r => (r._1)),
//                              position.get.asInstanceOf[Lineage[(Long, Int)]],
//                position.get.firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
//                  .map(r=> (r._1.get(), r._2.toString))
//              ).asInstanceOf[Lineage[(RecordId, String)]].cache() // Added dummy id. To be removed
            //)
        case _: TapParallelCollectionLRDD[_] =>
          result = new ShowRDD(
              position.get.asInstanceOf[Lineage[(RecordId, Int)]]
                .map(r=> ((Dummy, r._2), r._1.toString)).cache()
            )
        case _: TapPostCoGroupLRDD[_] =>
          val part = new LocalityAwarePartitioner(
            position.get.getCachedData.setCaptureLineage(true).partitions.size)
          val left = rightJoin(
            prev.asInstanceOf[Lineage[((Int, Int), Any)]].map(r => ((Dummy, r._1._2), r._2)),
            position.get.asInstanceOf[Lineage[(Int, Any)]].map(r => ((Dummy, r._1), r._2))
          ).map(r => (r._2.asInstanceOf[(CompactBuffer[Int], Int)], r._1)).flatMap(r => r._1._1.map(r2 => ((r2, r._1._2), r._2)))
          val right =
            position.get.getCachedData.setCaptureLineage(true).map {
              case (v, Array(vs, w1s)) =>
                (
                  (v, vs.asInstanceOf[Iterable[(_, Int)]]),
                  (v, w1s.asInstanceOf[Iterable[(_, Int)]])
                  )
            }.flatMap(
                r => for(v <- r.productIterator) yield v.asInstanceOf[(_, Iterable[(_, Int)])]
              ).flatMap( r => for(v <- r._2) yield((v._2, r._1.hashCode()), ((r._1, v._1), v._2).toString()))

          result = new ShowRDD(rightJoin[RecordId, Any](
            left, right).cache()
          )
        case _: TapPreCoGroupLRDD[_] =>
          val part = new LocalityAwarePartitioner(prev.partitions.size)
          val right = new ShuffledLRDD[RecordId, Any, Any](position.get.getCachedData.setCaptureLineage(true)
            .asInstanceOf[CoGroupedLRDD[_]].map {
              case (v, Array(vs, w1s)) =>
                ((v, vs.asInstanceOf[Iterable[(_, (_, Int))]]),
                  (v, w1s.asInstanceOf[Iterable[(_, Int)]]))
            }.flatMap(
              r => for(v <- r.productIterator) yield v.asInstanceOf[(_, Iterable[(_, Int)])]
            ).flatMap( r => for(v <- r._2) yield((v._2, r._1.hashCode()), ((r._1, v._1), v._2).toString())), part).map(r => ((r._1._1, r._1._2), r._2))

          result = new ShowRDD(rightJoin(
              prev.asInstanceOf[Lineage[(RecordId, Any)]].map(r => ((r._1._1, r._1._2), r._2)), right
            )//.map(r => ((0, r._1), r._2))
            .cache())
        case pre : TapPreShuffleLRDD[(Any, (Any, Int)) @unchecked] =>
          val current = if(!lineageContext.getlastOperation.isDefined) {
            prev
          } else {
              val part = new HashPartitioner(prev.partitions.size)
              new ShuffledLRDD[Int, Any, Any](prev.map(r => (r._1._2, r)), part)
                .map(r => r._2)
          }

          result = new ShowRDD(rightJoin[RecordId, String](current, pre.getCachedData.map { r =>
              val hash = r._1.hashCode()
              ((r._2._2, hash), ((r._1, r._2._1), hash).toString())
            })).cache()
        case post : TapPostShuffleLRDD[(Any, Int) @unchecked] =>
          val current: Lineage[(Int, Any)] = if(!lineageContext.getlastOperation.isDefined) {
            prev.asInstanceOf[Lineage[(RecordId, (CompactBuffer[Int], Int))]].map(r => (r._2._2, r._1))
          } else {
            val tmp = lineageContext.getLastLineageSeen.get match {
              case _: TapPreShuffleLRDD[_] | _: TapPostShuffleLRDD[_] => prev
              case _: TapLRDD[_] => rightJoin[Int, Any](prev.map(r => (r._1._2, r._2)), post)
            }
            lineageContext.getlastOperation.get match {
              case Direction.FORWARD => tmp.map(r => (r._2, r._1)).asInstanceOf[Lineage[(Int, Any)]]
              case _ => tmp.map(r => (r._2.asInstanceOf[(CompactBuffer[Int], Int)]._2, r._1))
            }
          }

          result = new ShowRDD(rightJoin[Int, Any](current,post.getCachedData.map(r => (r._1.hashCode, r)))
            .map(r => r._2).mapPartitions((iter: Iterator[Any]) =>
            post.getCachedData.getAggregate(iter.asInstanceOf[Iterator[Nothing]], null)).asInstanceOf[Lineage[(Any, Any)]]
            .map(r => ((Dummy, r._1.hashCode()), r.toString))
          ).cache()

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
