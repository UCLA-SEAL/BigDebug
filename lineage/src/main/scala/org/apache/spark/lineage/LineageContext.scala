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

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf, TextInputFormat}
import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.lineage.Direction.Direction
import org.apache.spark.lineage.rdd._
import org.apache.spark.rdd._

import scala.collection.mutable.{ Stack, HashSet }
import scala.language.implicitConversions
import scala.reflect.ClassTag

class LineageContext(@transient val sparkContext: SparkContext)
  extends Logging {

  implicit def fromRDDtoLineage(rdd: RDD[_]) = rdd.asInstanceOf[Lineage[_]]

  /**
   * Read a text file from HDFS, a local file system (available on all nodes), or any
   * Hadoop-supported file system URI, and return it as an RDD of Strings.
   */
  def textFile(path: String, minPartitions: Int = sparkContext.defaultMinPartitions): Lineage[String] = {
    hadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text],
      minPartitions).map(pair => pair._2.toString).setName(path)
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
  def hadoopLRDD[K, V](
     conf: JobConf,
     inputFormatClass: Class[_ <: InputFormat[K, V]],
     keyClass: Class[K],
     valueClass: Class[V],
     minPartitions: Int = sparkContext.defaultMinPartitions
     ): Lineage[(K, V)] = {
    // Add necessary security credentials to the JobConf before broadcasting it.
    SparkHadoopUtil.get.addCredentials(conf)

    val rdd = new HadoopLRDD(this, conf, inputFormatClass, keyClass, valueClass, minPartitions)
    if(isLineageActive) {
      rdd.tapRight()
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
      ): Lineage[(K, V)] = {
    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = sparkContext.broadcast(new SerializableWritable(sparkContext.hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)

    val rdd = new HadoopLRDD(
      this,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
    if(isLineageActive) {
      rdd.tapRight()
    } else {
      rdd
    }
  }

  /** Distribute a local Scala collection to form an RDD.
    *
    * @note Parallelize acts lazily. If `seq` is a mutable collection and is
    * altered after the call to parallelize and before the first action on the
    * RDD, the resultant RDD will reflect the modified collection. Pass a copy of
    * the argument to avoid this.
    */
  def parallelize[T: ClassTag](seq: Seq[T], numSlices: Int = sparkContext.defaultParallelism): Lineage[T] = {
    val rdd = new ParallelCollectionLRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
    if(isLineageActive) {
      rdd.tapRight()
    } else {
      rdd
    }
  }

  /**
   * Run a job on all partitions in an RDD and return the results in an array.
   */
  def runJob[T: ClassTag, U: ClassTag](rdd: Lineage[T], func: Iterator[T] => U): Array[U] = {
    val tappedRdd = tapJob(rdd)
    sparkContext.runJob(tappedRdd, func, 0 until tappedRdd.partitions.size, false)
  }

  def getLineage(rdd: Lineage[_]) = {
    val initialTap: Lineage[_] = rdd.materialize

    def visit(rdd: RDD[_], parent: RDD[_]) {
      rdd.setCaptureLineage(isLineageActive)
      var dependencies = List[OneToOneDependency[_]]()
      for (dep <- rdd.dependencies) {
        val newParent: RDD[_] = dep.rdd match {
          case tap: TapLRDD[_] =>
            dependencies = new OneToOneDependency(tap.materialize.cache()) :: dependencies
            tap
          case _ => parent
        }
        visit(dep.rdd, newParent)
      }
      if(!dependencies.isEmpty) {
        val oldDeps = parent.dependencies.filter(d => d.rdd.isInstanceOf[TapLRDD[_]])
        parent.updateDependencies(oldDeps.toList ::: dependencies)
      }
    }

    visit(initialTap, initialTap)

    currentLineagePosition = Some(rdd.asInstanceOf[Lineage[((Int, Int, Long), Any)]])
  }

  private def tapJob[T](rdd: Lineage[T]): RDD[T] = {
    if(!isLineageActive) {
      return rdd
    }
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        val deps = new HashSet[Dependency[_]]
        for (dep <- rdd.dependencies) {
          waitingForVisit.push(dep.rdd)
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              shufDep.rdd.setTap(rdd.tapLeft())
              deps += shufDep.tapDependency(shufDep.rdd.getTap().get)

            case narDep: OneToOneDependency[_] =>
              // Intercept the end of the stage to add a post-shuffle tap
              if(narDep.rdd.dependencies.nonEmpty) {
                if(narDep.rdd.dependencies
                  .filter(d => d.isInstanceOf[ShuffleDependency[_, _, _]])
                  .count(d => d.rdd.getTap() == None) > 0) {
                  val tap = narDep.rdd.tapRight()
                  deps += narDep.tapDependency(tap)
                }
              }

            case _ =>
          }
        }
        if(deps.nonEmpty) {
          rdd.updateDependencies(deps.toList)
        }
      }
    }
    waitingForVisit.push(rdd)
    while (!waitingForVisit.isEmpty) {
      visit(waitingForVisit.pop())
    }

    rdd.tapRight()
  }

  private var captureLineage: Boolean = false

  private var currentLineagePosition: Option[Lineage[_]] = None

  private var lastLineagePosition: Option[Lineage[_]] = None

  def getCurrentLineagePosition = currentLineagePosition

  def setCurrentLineagePosition(initialRDD: Option[Lineage[_]]) = {
    if(lastLineagePosition.isDefined && lastLineagePosition.get != initialRDD.get) {
      currentLineagePosition = lastLineagePosition

      // We are starting from the middle, fill the stack with prev positions
      while(currentLineagePosition.get != initialRDD.get) {
        prevLineagePosition.push(currentLineagePosition.get)
        currentLineagePosition = Some(currentLineagePosition.get.dependencies(0).rdd)
      }
    }
    currentLineagePosition = initialRDD
  }

  def setLastLineagePosition(finalRDD: Option[Lineage[_]]) = lastLineagePosition = finalRDD

  private[spark] var prevLineagePosition = new Stack[Lineage[_]]()

  private[spark] var lastOperation: Option[Direction] = None

  def isLineageActive: Boolean = captureLineage

  def setCaptureLineage(newLineage: Boolean) = {
    if(newLineage == false && captureLineage == true) {
      getLineage(lastLineagePosition.get)
    }
    captureLineage = newLineage
  }

  def getBackward(path: Int = 0) = {
    // CurrentLineagePosition should be always set at this point
    if(currentLineagePosition.get.dependencies.size == 0 ||
      currentLineagePosition.get.dependencies(path).rdd.isInstanceOf[HadoopRDD[_, _]]) {
      throw new UnsupportedOperationException("unsopported operation")
    }

    prevLineagePosition.push(currentLineagePosition.get)

    currentLineagePosition = Some(currentLineagePosition.get.dependencies(path).rdd)

    if(!lastOperation.isDefined || lastOperation.get == Direction.FORWARD) {
      lastOperation = Some(Direction.BACKWARD)
      None
    } else {
      var result = prevLineagePosition.head
      if (prevLineagePosition.head.dependencies.size > 1) {
        // Filter the correct values. Required because lastOperation is a join
        val filter = currentLineagePosition.get.id
        result = result.asInstanceOf[RDD[((Int, Int, Long), (Int, Int, Long))]].filter(r => r._2._1.equals(filter))
      }
      Some(result.asInstanceOf[Lineage[((Int, Int, Long), Any)]])
    }
  }

  def getForward: Lineage[((Int, Int, Long), Any)] = {
    if(!lastOperation.isDefined || lastOperation.get == Direction.BACKWARD) {
      lastOperation = Some(Direction.FORWARD)
    }
    if(prevLineagePosition.isEmpty) {
      throw new UnsupportedOperationException("unsopported operation")
    }

    currentLineagePosition = Some(prevLineagePosition.pop())

    currentLineagePosition
      .get
      .asInstanceOf[Lineage[((Int, Int, Long), (Int, Int, Long))]]
      .map(r => (r._2, r._1))
  }
}

object Direction extends Enumeration {
  type Direction = Value
  val FORWARD, BACKWARD = Value
}

object LineageContext {
  implicit def lRDDToPairLRDDFunctions[K, V](lrdd: Lineage[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null) = {
    new PairLRDDFunctions(lrdd)
  }
}