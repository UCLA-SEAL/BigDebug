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

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf, OutputFormat}
import org.apache.spark.Partitioner._
import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.executor.OutputMetrics
import org.apache.spark.lineage.LAggregator
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.rdd._
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.collection.CompactBuffer

import scala.language.implicitConversions
import scala.reflect.ClassTag

private[spark] class PairLRDDFunctions[K, V](self: Lineage[(K, V)])
(implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
  extends PairRDDFunctions[K, V](self) {

  def lineageContext = self.lineageContext

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  override def cogroup[W](other: RDD[(K, W)]): Lineage[(K, (Iterable[V], Iterable[W]))] = {
    cogroup(other, defaultPartitioner(self, other))
  }

  /**
   * For each key k in `this` or `other`, return a resulting RDD that contains a tuple with the
   * list of values for that key in `this` as well as `other`.
   */
  override def cogroup[W](other: RDD[(K, W)], partitioner: Partitioner)
  : Lineage[(K, (Iterable[V], Iterable[W]))]  = {
    if (partitioner.isInstanceOf[HashPartitioner] && keyClass.isArray) {
      throw new SparkException("Default partitioner cannot partition array keys.")
    }
    val cg = new CoGroupedLRDD[K](Seq(self, other), partitioner)
    cg.mapValues { case Array(vs, w1s) =>
      (vs.asInstanceOf[Iterable[V]], w1s.asInstanceOf[Iterable[W]])
    }
  }

  /**
   * Generic function to combine the elements for each key using a custom set of aggregation
   * functions. Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C
   * Note that V and C can be different -- for example, one might group an RDD of type
   * (Int, Int) into an RDD of type (Int, Seq[Int]). Users provide three functions:
   *
   * - `createCombiner`, which turns a V into a C (e.g., creates a one-element list)
   * - `mergeValue`, to merge a V into a C (e.g., adds it to the end of a list)
   * - `mergeCombiners`, to combine two C's into a single one.
   *
   * In addition, users can control the partitioning of the output RDD, and whether to perform
   * map-side aggregation (if a mapper can produce multiple items with the same key).
   */
  override def combineByKey[C](createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null): Lineage[(K, C)] = {
    require(mergeCombiners != null, "mergeCombiners must be defined") // required as of Spark 0.9.0
    if (keyClass.isArray) {
      if (mapSideCombine) {
        throw new SparkException("Cannot use map-side combining with array keys.")
      }
      if (partitioner.isInstanceOf[HashPartitioner]) {
        throw new SparkException("Default partitioner cannot partition array keys.")
      }
    }
    val aggregator = new LAggregator[K, V, C](
      createCombiner,
      mergeValue,
      mergeCombiners,
      lineageContext.isLineageActive)

    new ShuffledLRDD[K, V, C](self, partitioner)
      .setSerializer(serializer)
      .setAggregator(aggregator)
      .asInstanceOf[ShuffledLRDD[K, V, C]]
      .setMapSideCombine(mapSideCombine)
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with the existing partitioner/parallelism level. The ordering of elements
   * within each group is not guaranteed, and may even differ each time the resulting RDD is
   * evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  override def groupByKey(): Lineage[(K, Iterable[V])] = {
    groupByKey(defaultPartitioner(self))
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Hash-partitions the
   * resulting RDD with into `numPartitions` partitions. The ordering of elements within
   * each group is not guaranteed, and may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  override def groupByKey(numPartitions: Int): Lineage[(K, Iterable[V])] = {
    groupByKey(new HashPartitioner(numPartitions))
  }

  /**
   * Group the values for each key in the RDD into a single sequence. Allows controlling the
   * partitioning of the resulting key-value pair RDD by passing a Partitioner.
   * The ordering of elements within each group is not guaranteed, and may even differ
   * each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  override def groupByKey(partitioner: Partitioner): Lineage[(K, Iterable[V])] = {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: V) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    val bufs = combineByKey[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
    bufs.asInstanceOf[Lineage[(K, Iterable[V])]]
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with the existing partitioner/
   * parallelism level.
   */
  override def reduceByKey(func: (V, V) => V): Lineage[(K, V)] = {
    reduceByKey(defaultPartitioner(self), func)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce.
   */
  override def reduceByKey(partitioner: Partitioner, func: (V, V) => V): Lineage[(K, V)] = {
    combineByKey[V]((v: V) => v, func, func, partitioner)
  }

  /**
   * Merge the values for each key using an associative reduce function. This will also perform
   * the merging locally on each mapper before sending results to a reducer, similarly to a
   * "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
   */
  override def reduceByKey(func: (V, V) => V, numPartitions: Int): Lineage[(K, V)] = {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }

  /**
   * Return an RDD with the values of each tuple.
   */
  override def values: Lineage[V] = self.map(_._2)

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Uses the given Partitioner to partition the output RDD.
   */
  override def join[W](other: RDD[(K, W)], partitioner: Partitioner): Lineage[(K, (V, W))] = {
    this.cogroup(other, partitioner).flatMapValues( pair =>
      for (v <- pair._1; w <- pair._2) yield (v, w)
    )
  }

  /**
   * Return an RDD containing all pairs of elements with matching keys in `this` and `other`. Each
   * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in `this` and
   * (k, v2) is in `other`. Performs a hash join across the cluster.
   */
  override def join[W](other: RDD[(K, W)]): Lineage[(K, (V, W))] = {
    join(other, defaultPartitioner(self, other))
  }

  /**
   * Pass each value in the key-value pair RDD through a map function without changing the keys;
   * this also retains the original RDD's partitioning.
   */
  override def mapValues[U](f: V => U): Lineage[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MapPartitionsLRDD[(K, U), (K, V)](self,
      (context, pid, iter) => iter.map { case (k, v) => (k, cleanF(v)) },
      preservesPartitioning = true)
  }

  /**
   * Pass each value in the key-value pair RDD through a flatMap function without changing the
   * keys; this also retains the original RDD's partitioning.
   */
  override def flatMapValues[U](f: V => TraversableOnce[U]): Lineage[(K, U)] = {
    val cleanF = self.context.clean(f)
    new MapPartitionsLRDD[(K, U), (K, V)](self,
      (context, pid, iter) => iter.flatMap { case (k, v) =>
        cleanF(v).map(x => (k, x))
      },
      preservesPartitioning = true)
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   */
  override def saveAsHadoopFile[F <: OutputFormat[K, V]](path: String)(implicit fm: ClassTag[F]) {
    saveAsHadoopFile(path, keyClass, valueClass, fm.runtimeClass.asInstanceOf[Class[F]])
  }

  /**
   * Output the RDD to any Hadoop-supported file system, using a Hadoop `OutputFormat` class
   * supporting the key and value types K and V in this RDD.
   * Copied from PairRDDFunctions
   */
  override def saveAsHadoopFile(
      path: String,
      keyClass: Class[_],
      valueClass: Class[_],
      outputFormatClass: Class[_ <: OutputFormat[_, _]],
      conf: JobConf = new JobConf(self.context.hadoopConfiguration),
      codec: Option[Class[_ <: CompressionCodec]] = None) {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    hadoopConf.setOutputKeyClass(keyClass)
    hadoopConf.setOutputValueClass(valueClass)
    // Doesn't work in Scala 2.9 due to what may be a generics bug
    // TODO: Should we uncomment this for Scala 2.10?
    // conf.setOutputFormat(outputFormatClass)
    hadoopConf.set("mapred.output.format.class", outputFormatClass.getName)
    for (c <- codec) {
      hadoopConf.setCompressMapOutput(true)
      hadoopConf.set("mapred.output.compress", "true")
      hadoopConf.setMapOutputCompressorClass(c)
      hadoopConf.set("mapred.output.compression.codec", c.getCanonicalName)
      hadoopConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
    }

    // Use configured output committer if already set
    if (conf.getOutputCommitter == null) {
      hadoopConf.setOutputCommitter(classOf[FileOutputCommitter])
    }

    FileOutputFormat.setOutputPath(hadoopConf,
      SparkHadoopWriter.createPathFromString(path, hadoopConf))
    saveAsHadoopDataset(hadoopConf)
  }

  /**
   * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for
   * that storage system. The JobConf should set an OutputFormat and any output paths required
   * (e.g. a table name to write to) in the same way as it would be configured for a Hadoop
   * MapReduce job.
   * Copied from PairRDDFunctions
   */
  override def saveAsHadoopDataset(conf: JobConf) {
    // Rename this as hadoopConf internally to avoid shadowing (see SPARK-2038).
    val hadoopConf = conf
    val wrappedConf = new SerializableWritable(hadoopConf)
    val outputFormatInstance = hadoopConf.getOutputFormat
    val keyClass = hadoopConf.getOutputKeyClass
    val valueClass = hadoopConf.getOutputValueClass
    if (outputFormatInstance == null) {
      throw new SparkException("Output format class not set")
    }
    if (keyClass == null) {
      throw new SparkException("Output key class not set")
    }
    if (valueClass == null) {
      throw new SparkException("Output value class not set")
    }
    SparkHadoopUtil.get.addCredentials(hadoopConf)

    logDebug("Saving as hadoop file of type (" + keyClass.getSimpleName + ", " +
      valueClass.getSimpleName + ")")

    if (isOutputSpecValidationEnabled) {
      // FileOutputFormat ignores the filesystem parameter
      val ignoredFs = FileSystem.get(hadoopConf)
      hadoopConf.getOutputFormat.checkOutputSpecs(ignoredFs, hadoopConf)
    }

    val writer = new SparkHadoopWriter(hadoopConf)
    writer.preSetup()

    val writeToFile = (context: TaskContext, iter: Iterator[(K, V)]) => {
      val config = wrappedConf.value
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.taskAttemptId % Int.MaxValue).toInt

      val outputMetricsAndBytesWrittenCallback: Option[(OutputMetrics, () => Long)] =
        initHadoopOutputMetrics(context)

      writer.setup(context.stageId, context.partitionId, attemptNumber)
      writer.open()
      try {
        var recordsWritten = 0L
        while (iter.hasNext) {
          val record = iter.next()
          writer.write(record._1.asInstanceOf[AnyRef], record._2.asInstanceOf[AnyRef])

          // Update bytes written metric every few records
          maybeUpdateOutputMetrics(outputMetricsAndBytesWrittenCallback, recordsWritten)
          recordsWritten += 1
        }
      } finally {
        writer.close()
      }
      writer.commit()
      outputMetricsAndBytesWrittenCallback.foreach { case (om, callback) =>
        om.setBytesWritten(callback())
      }
    }

    self.lineageContext.runJob(self, writeToFile)
    writer.commitJob()
  }
}
