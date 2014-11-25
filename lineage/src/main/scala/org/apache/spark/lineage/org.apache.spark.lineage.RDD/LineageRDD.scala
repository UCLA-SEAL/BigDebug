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

import org.apache.hadoop.io.{Text, LongWritable}

import org.apache.spark.lineage.Direction.Direction
import org.apache.spark.lineage.{SchemaShufflePost, SchemaShow, Direction, LineageContext}
import org.apache.spark.rdd._
import org.apache.spark.sql.DataType
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.types.{DataType, StringType}
import org.apache.spark.sql.{Row, DataType, SchemaRDD}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.{Partition, TaskContext}

private[spark]
class LineageRDD(@transient lc: LineageContext, @transient prev: SchemaRDD)
  extends RDD[String](prev) {

  import lc.sqlContext._

  prev.registerTempTable("schema_" + prev.id)
  cacheTable("schema_" + prev.id)

  override def getPartitions: Array[Partition] = firstParent[((Int, Int, Long), Any)].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[(String, Any)].iterator(split, context).map(r => r._1)

  override def collect() = {
    prev.map(r => (r(0).toString)).collect()
  }

  override def filter(f: (String) => Boolean): LineageRDD = {
    new LineageRDD(lc, prev.filter(r => f(r(0).toString)))
  }

  def goNext(): LineageRDD = {
    var forward = lc.getForward.id
      val schema = lc.sqlContext.sql(
        "SELECT * " +
        "FROM schema_" + forward + " x " +
        "LEFT SEMI JOIN schema_" + prev.id + " y " +
        "ON (x.value = y.key)")
      new LineageRDD(lc, schema)
  }

  def goBack(): LineageRDD = {
    val next = lc.getBackward
    if (next.isDefined) {
     new LineageRDD(lc, lc.sqlContext.sql(
        "SELECT DISTINCT * " +
        "FROM schema_" + next.get.id + " x " +
        "LEFT SEMI JOIN schema_" + prev.id + " y " +
        "ON (x.key = y.value)"))
    } else {
      lc.sqlContext.table("schema_" + prev.id).collect().foreach(println)
      new LineageRDD(lc, lc.sqlContext.sql(
        "SELECT DISTINCT x.value AS key, x.key AS value " +
        "FROM schema_" + prev.id + " x"))
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
    result
  }

  def goBack(times: Int = 1) = go(times, Direction.BACKWARD)

  def goNext(times: Int = 1) = go(times)

  def goBackAll() = go(Int.MaxValue, Direction.BACKWARD)

  def goNextAll() = go(Int.MaxValue)

  def show(): ShowRDD = {
    val position = lc.getCurrentLineagePosition
    if(position.isDefined) {
      var result: ShowRDD = null

      if (position.get.isInstanceOf[TapHadoopRDD[_, _]]) {
        lc.getSchemaRDD(position.get.firstParent.asInstanceOf[HadoopRDD[LongWritable, Text]])
        result = new ShowRDD(lc, lc.sqlContext.sql(
          "SELECT x.key AS key, z.value AS value " +
            "FROM schema_" + prev.id + " x " +
          "INNER JOIN schema_" + position.get.firstParent.id + " z ON (x.value._2 = z.key)"))
      } else if(position.get.isInstanceOf[TapPreShuffleRDD[_]]) {
        val cache = position.get.asInstanceOf[TapPreShuffleRDD[_]].getCached
        val toShowSchema = (arg2: Any, arg3: Any, arg4: Long) => ((arg2, arg3), arg4).toString()

        registerFunction("toShowSchema3", toShowSchema)
        lc.getSchemaRDD(cache)

        result = new ShowRDD(lc, lc.sqlContext.sql(
          "SELECT x.value._2 AS key, toShowSchema3(x.key, x.value._1, x.value._2._3) AS value " +
            "FROM schema_" + cache.id + " x " +
            "LEFT SEMI JOIN schema_" + prev.id + " y " +
            "ON (x.value._2 = y.key)"
        ))
      } else if(position.get.isInstanceOf[TapPostShuffleRDD[_]]) {
        val toShowSchema = (arg2: Any, arg3: Long) => (arg2, arg3).toString()

        registerFunction("toShowSchema2", toShowSchema)
        val cache = position.get.asInstanceOf[TapPostShuffleRDD[_]].getCached.setCaptureLineage(false)
          .asInstanceOf[ShuffledRDD[_, _, _]]
          .map(r => SchemaShufflePost(r._1.toString, r._2.asInstanceOf[(Int, Int, Long)]))
        cache.registerTempTable("schema_" + cache.id)

        result = new ShowRDD(lc, lc.sqlContext.sql(
          "SELECT x.value AS key, toShowSchema2(x.key, x.value._3) AS value " +
            "FROM schema_" + cache.id + " x " +
            "LEFT SEMI JOIN schema_" + prev.id + " y " +
            "ON (x.value = y.key)"
        ))
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
