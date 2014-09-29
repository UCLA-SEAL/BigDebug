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
// Added by Miao

package org.apache.spark.rdd

import org.apache.spark._

private[spark]
class TapHadoopRDD[K, V](
    sc: SparkContext,
    deps: Seq[Dependency[_]])
  extends TapRDD[(K, V)](sc, deps) {

  def this(@transient prev: HadoopRDD[_, _]) = {
    this(prev.context, List(new OneToOneDependency(prev)))
  }

  override def tap(record: (K, V)) = {
    //val a: Long = record._1.asInstanceOf[LongWritable].get
    //val b: String = record._2.asInstanceOf[Text].toString
    //val cloned = (a, b).asInstanceOf[(K, V)]

    val hadoopRDD = firstParent[(K, V)].asInstanceOf[HadoopRDD[K, V]]
    val offset = hadoopRDD.getReader.getPos() - record._2.toString.size - 1
    val tuple2 = (hadoopRDD.getFilePath, offset)
    val recordId = (id, splitId, newRecordId)
    tContext.currentRecordInfo = Seq(recordId)
    addRecordInfo(recordId, Seq(tuple2))
    //SparkEnv.get.cacheManager.materialize(this, (id, tuple2))
    // println("Tapping " + record + " with id " + id + " joins with " + tuple2)
    record
  }
}
