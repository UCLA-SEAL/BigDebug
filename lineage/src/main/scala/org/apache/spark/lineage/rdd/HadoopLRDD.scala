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
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.lineage.{Lineage, LineageContext}
import org.apache.spark.rdd.HadoopRDD

import scala.reflect._


/**
 * :: DeveloperApi ::
 * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
 * sources in HBase, or S3), using the older MapReduce API (`org.apache.hadoop.mapred`).
 *
 * Note: Instantiating this class directly is not recommended, please use
 * [[org.apache.spark.SparkContext.hadoopRDD()]]
 *
 * @param lc The LineageContext to associate the LRDD with.
 * @param broadcastedConf A general Hadoop Configuration, or a subclass of it. If the enclosed
 *     variabe references an instance of JobConf, then that JobConf will be used for the Hadoop job.
 *     Otherwise, a new JobConf will be created on each slave using the enclosed Configuration.
 * @param initLocalJobConfFuncOpt Optional closure used to initialize any JobConf that HadoopRDD
 *     creates.
 * @param inputFormatClass Storage format of the data to be read.
 * @param keyClass Class of the key associated with the inputFormatClass.
 * @param valueClass Class of the value associated with the inputFormatClass.
 * @param minPartitions Minimum number of HadoopRDD partitions (Hadoop Splits) to generate.
 */
@DeveloperApi
class HadoopLRDD[K, V](
    @transient lc: LineageContext,
    broadcastedConf: Broadcast[SerializableWritable[Configuration]],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int)
  extends HadoopRDD[K, V](
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
      minPartitions: Int) = {
    this(
      lc,
      lc.sparkContext.broadcast(new SerializableWritable(conf))
        .asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      None /* initLocalJobConfFuncOpt */,
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions)
  }

  override def ttag = classTag[(K, V)]

  override def lineageContext: LineageContext = lc

  private var filePath: String = null

  override def tapRight(): TapHadoopLRDD[K, V] = {
    new TapHadoopLRDD(this).asInstanceOf[TapHadoopLRDD[K, V]]
  }

  override def setName(_name: String): this.type = {
    name = _name
    filePath = _name
    this
  }

  def getFilePath = filePath

  def getReader = reader
}
