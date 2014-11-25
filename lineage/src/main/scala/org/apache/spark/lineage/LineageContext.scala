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

package org.apache.spark.lineage

import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.{TextInputFormat, InputFormat, JobConf, FileInputFormat}
import org.apache.spark.lineage.Direction.Direction
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.lineage.rdd.LineageRDD
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.{SchemaRDD, SQLContext}

import scala.collection.mutable.{HashSet, Stack}
import scala.language.implicitConversions

case class SchemaTap(key: (Int, Int, Long), value: (Int, Int, Long))

case class SchemaTapHaddop(key: (Int, Int, Long), value: (String, Long))

case class SchemaShow(key: (Int, Int, Long), value: String)

case class SchemaHaddop(key: Long, value: String)

case class SchemaShuffle(key: String, value: (String, (Int, Int, Long)))

case class SchemaShufflePost(key: String, value: (Int, Int, Long))

class LineageContext(@transient val sparkContext: SparkContext)
  extends Logging {

  @transient val sqlContext = new SQLContext(sparkContext)

  // Importing the SQL context gives access to all the SQL functions and implicit conversions.
  import sqlContext._

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   */
  def textFile(path: String, minPartitions: Int = sparkContext.defaultMinPartitions): RDD[String] = {
    val hadoop = hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions)
    val map = hadoop.map(pair => pair._2.toString).setName(path)
    map.setTap(hadoop.asInstanceOf[TapHadoopRDD[_, _]])
    map
  }

  /**
   * Get an RDD for a Hadoop-readable dataset from a Hadoop JobConf given its InputFormat and other
   * necessary info (e.g. file name for a filesystem-based dataset, table name for HyperTable),
   * using the older MapReduce API (`org.apache.hadoop.mapred`).
   *
   * @param conf JobConf for setting up the dataset
   * @param inputFormatClass Class of the InputFormat
   * @param keyClass Class of the keys
   * @param valueClass Class of the values
   * @param minPartitions Minimum number of Hadoop Splits to generate.
   *
   * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
   * record, directly caching the returned RDD will create many references to the same object.
   * If you plan to directly cache Hadoop writable objects, you should first copy them using
   * a `map` function.
   */
  def hadoopRDD[K, V](
     conf: JobConf,
     inputFormatClass: Class[_ <: InputFormat[K, V]],
     keyClass: Class[K],
     valueClass: Class[V],
     minPartitions: Int = sparkContext.defaultMinPartitions
     ): RDD[(K, V)] = {
    // Add necessary security credentials to the JobConf before broadcasting it.
    SparkHadoopUtil.get.addCredentials(conf)

    /* Modified by Miao */
    val rdd = new HadoopRDD(sparkContext, conf, inputFormatClass, keyClass, valueClass, minPartitions)
    if(isLineageActive) {
      rdd.tap()
    } else {
      rdd
    }
  }

  /** Get an RDD for a Hadoop file with an arbitrary InputFormat
    *
    * '''Note:''' Because Hadoop's RecordReader class re-uses the same Writable object for each
    * record, directly caching the returned RDD will create many references to the same object.
    * If you plan to directly cache Hadoop writable objects, you should first copy them using
    * a `map` function.
    * */
  def hadoopFile[K, V](
    path: String,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int = sparkContext.defaultMinPartitions
    ): RDD[(K, V)] = {
    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = sparkContext.broadcast(new SerializableWritable(sparkContext.hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)

    /* Modified by Miao */
    val rdd = new HadoopRDD(
      sparkContext,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
    if(isLineageActive) {
      rdd.tap()
    } else {
      rdd
    }
  }

  def getLineage(rdd: RDD[_]): LineageRDD = {
    if(rdd.getTap().isDefined) {
      setCurrentLineagePosition(rdd.getTap())
      return new LineageRDD(this, getSchemaRDD(rdd.getTap().get))
    }
    throw new UnsupportedOperationException("no lineage support for this RDD")
  }

  def getSchemaRDD(rdd: RDD[_]):SchemaRDD = {
    rdd match {
      case _:TapHadoopRDD[_, _] =>
        val result = rdd
          .asInstanceOf[RDD[((Int, Int, Long), (String, Long))]]
          .map(r => SchemaTapHaddop(r._1, r._2))
        result.registerTempTable("schema_" + rdd.id)
        cacheTable("schema_" + rdd.id)
        result
      case _:TapRDD[_] =>
        val result = rdd
          .asInstanceOf[RDD[((Int, Int, Long), (Int, Int, Long))]]
          .map(r => SchemaTap(r._1, r._2))
        result.registerTempTable("schema_" + rdd.id)
        cacheTable("schema_" + rdd.id)
        result
      case _:HadoopRDD[_, _] =>
        val result = rdd
          .asInstanceOf[HadoopRDD[LongWritable, Text]]
          .map(r => SchemaHaddop(r._1.get, r._2.toString))
        result.registerTempTable("schema_" + rdd.id)
        result
      case _:ShuffledRDD[_, _, _] =>
        val result = rdd
          .asInstanceOf[ShuffledRDD[_, _, _]]
          .map(r => SchemaShuffle(r._1.toString, r._2.asInstanceOf[(String, (Int, Int, Long))]))
        result.registerTempTable("schema_" + rdd.id)
        result
      case _:RDD[_] =>
        val result = rdd
          .asInstanceOf[RDD[((Int, Int, Long), String)]]
          .map(r => SchemaShow(r._1, r._2))
        result.registerTempTable("schema_" + rdd.id)
        result
    }
  }

  private var captureLineage: Boolean = false

  private var currentLineagePosition: Option[RDD[_]] = None

  private var lastLineagePosition: Option[RDD[_]] = None

  def getCurrentLineagePosition = currentLineagePosition

  def setCurrentLineagePosition(initialRDD: Option[RDD[_]]) = {
    // We are starting from the middle, fill the stack with prev positions
    if(lastLineagePosition.isDefined && lastLineagePosition.get != initialRDD.get) {
      currentLineagePosition = lastLineagePosition
      while(currentLineagePosition.get != initialRDD.get) {
        prevLineagePosition.push(currentLineagePosition.get)
        currentLineagePosition = Some(currentLineagePosition.get.dependencies(0).rdd)
      }
    }
    currentLineagePosition = initialRDD
  }

  private[spark] var prevLineagePosition = new Stack[RDD[_]]()

  private[spark] var lastOperation: Option[Direction] = None

  def isLineageActive: Boolean = captureLineage

  def setCaptureLineage(newLineage: Boolean) = {
    if(newLineage == false && captureLineage == true) {
      lastLineagePosition = Some(sparkContext.getLasLineagePostion.get)
      computeLineageDependecies(lastLineagePosition.get)
    }
    captureLineage = newLineage
    sparkContext.setCaptureLineage(newLineage)
  }

  def getBackward = {
    if(!lastOperation.isDefined) {
      lastOperation = Some(Direction.BACKWARD)
    }

    if(currentLineagePosition.get.dependencies.size == 0 ||
      currentLineagePosition.get.dependencies(0).rdd.isInstanceOf[HadoopRDD[_, _]]) {
      throw new UnsupportedOperationException("unsopported operation")
    }

    prevLineagePosition.push(currentLineagePosition.get)

    currentLineagePosition = Some(currentLineagePosition.get.dependencies(0).rdd)

    if(lastOperation.get == Direction.FORWARD) {
      lastOperation = Some(Direction.BACKWARD)
      //None
    }
    //else {
      //Some(prevLineagePosition.head.asInstanceOf[RDD[((Int, Int, Long), Any)]])
      currentLineagePosition
    //}
  }

  def getForward = {
    if(!lastOperation.isDefined || lastOperation.get == Direction.BACKWARD) {
      lastOperation = Some(Direction.FORWARD)
    }
    if(prevLineagePosition.isEmpty) {
      throw new UnsupportedOperationException("unsopported operation")
    }

    currentLineagePosition = Some(prevLineagePosition.pop())

    currentLineagePosition.get
  }

  def computeLineageDependecies(rdd: RDD[_]) = {

    var initialTap: RDD[_] = rdd.materialize

    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    val dependencies = new Stack[RDD[_]]()
    dependencies.push(initialTap)

    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        rdd.setCaptureLineage(isLineageActive)
        rdd.dependencies
          .filter(_.rdd.isInstanceOf[TapRDD[_]])
          .foreach(d => dependencies.push(d.rdd.materialize))
        for (dep <- rdd.dependencies) {
          waitingForVisit.push(dep.rdd)
        }
      }
    }
    waitingForVisit.push(initialTap)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }

    initialTap = dependencies.pop()
    getSchemaRDD(initialTap)

    while (dependencies.size > 0) {
      dependencies.head.updateDependencies(Seq(new OneToOneDependency(initialTap)))
      initialTap = dependencies.pop()
      getSchemaRDD(initialTap)
    }

    currentLineagePosition = Some(initialTap.asInstanceOf[RDD[((Int, Int, Long), Any)]])
  }
}

object Direction extends Enumeration {
  type Direction = Value
  val FORWARD, BACKWARD = Value
}
