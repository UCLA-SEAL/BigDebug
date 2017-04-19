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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{InputFormat, JobConf}
import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.lineage.LineageContext
import org.apache.spark.rdd.HadoopRDD

import scala.reflect._

class HadoopLRDD[K, V](
    @transient lc: LineageContext,
    broadcastedConf: Broadcast[SerializableWritable[Configuration]],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int
  ) extends HadoopRDD[K, V](
    lc.sparkContext,
    broadcastedConf,
    initLocalJobConfFuncOpt,
    inputFormatClass,
    keyClass,
    valueClass,
    minPartitions) with Lineage[(K, V)] {

  def this(
      lc: LineageContext,
      conf: JobConf,
      inputFormatClass: Class[_ <: InputFormat[K, V]],
      keyClass: Class[K],
      valueClass: Class[V],
      minPartitions: Int
  ) = this(
      lc,
      lc.sparkContext.broadcast(new SerializableWritable(conf))
        .asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      None /* initLocalJobConfFuncOpt */,
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions
    )

  override def lineageContext: LineageContext = lc

  override def ttag = classTag[(K, V)]

  private var filePath: String = null

  override def tapRight(): TapHadoopLRDD[K, V] = new TapHadoopLRDD(this)

  override def setName(_name: String): this.type = {
    name = _name
    filePath = _name
    this
  }

  def getFilePath = filePath

  def getReader = reader
}
