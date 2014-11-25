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

import org.apache.spark.lineage.LineageContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.{Partition, TaskContext}

private[spark]
class ShowRDD(lc: LineageContext, prev: SchemaRDD)
  extends RDD[String](prev) {

  import lc.sqlContext._

  prev.registerTempTable("schema_" + prev.id)
  cacheTable("schema_" + prev.id)

  override def getPartitions: Array[Partition] = firstParent[((Int, Int, Long), Any)].partitions

  override def compute(split: Partition, context: TaskContext) =
    firstParent[((Int, Int, Long), String)].iterator(split, context).map(r => r._2)

  override def collect() = {
    prev.map(r => (r(1).toString)).collect()
  }

  override def filter(f: (String) => Boolean): ShowRDD = {
    new ShowRDD(lc, prev.filter(r => f(r(1).toString)))
  }

  def getLineage(): LineageRDD = {
    val result = new LineageRDD(lc, lc.sqlContext.sql(
      "SELECT DISTINCT * FROM schema_" + lc.getCurrentLineagePosition.get.id + " x " +
        "LEFT SEMI JOIN schema_" + prev.id + " y ON (x.key = y.key)"))
    result.collect().foreach(println)
    result
  }
}