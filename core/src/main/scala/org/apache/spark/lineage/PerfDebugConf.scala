package org.apache.spark.lineage

import org.apache.spark.SparkEnv

import scala.util.Try

/**
 * Primitive configuration class for storing various toggles. This is currently intended for use
 * only on the driver side (i.e., worker classes should be configured beforehand).
 * Created 11/26/2018
 *
 * @author jteoh
 */
case class PerfDebugConf(wrapUDFs: Boolean = true,
                         // TODO look for "shuffle flag (instrumentation toggle)" TODOs and complete them.
                         _estimateShuffleLatency: Boolean = false,
                         uploadLineage: Boolean = true, // default true
                         uploadLineageRecordsLimit: Int = -1,
                         _uploadBatchSize: Int = 100 * 1000, // default 100K
                         tapRDDs: Boolean = true,
                         materializeBuffers: Boolean = true,
                         allocateBuffers: Boolean = true,
                         uploadIgniteDataAfterConversion: Boolean = true,
                         _enableSparkContextPerfListenerPrinter: Boolean = true,
                         enableSparkContextPerfListenerIgniteStore: Boolean = false,
                         _experimentalPerfQueryFirstShuffleTrim: Option[Int] = Some(5),
                         experimentalPreShuffleLineageBound: Option[Int] = None //Some(10)
                        ) extends
  Serializable {
  if(experimentalPreShuffleLineageBound.isDefined) {
    println("Warning: pre shuffle lineage bound is experimental and WILL result in only partial " +
              "lineage upload. This should be used with caution and under the expectation that " +
              "slowest-input queries will be executed!")
  }
  if(tapRDDs && !allocateBuffers) {
    println("Warning: RDD lineage buffers are not allocated but tapping is still enabled - these " +
              "tags will not be stored")
    // NOTE: allocateBuffers is not actually used.
    // NOTE: tapRDDs is not actually used.
  }
  if(materializeBuffers && !allocateBuffers) {
    println("Warning: RDD lineage buffers are not allocated but materialization is still enabled.")
    // NOTE: allocateBuffers is not actually used.
  }
  
  if(uploadLineage && !materializeBuffers) {
    println("Warning: RDD lineage buffers are not materialized, but empty data is still uploaded " +
              "to ignite.")
  }
  
  if(uploadLineageRecordsLimit > 0 && !uploadLineage) {
    println("Warning: RDD lineage upload limit is ignored because lineage uploading is disabled.")
  }
  
  if(!uploadIgniteDataAfterConversion) {
    println("Warning: uploadIgniteDataAfterConversion flag is currently broken")
  }
  
  // TODO
  // let's see
  // 1: UDF Wrapping - I think this is done.
  // 2: Shuffle approximations. Not done yet. I think this should be pretty small in the grand
  // scheme of things.
  // 3: uploading to Ignite. Questionable contribution -. This is theoretically done in parallel.
  // 4: Tapping RDDs - the act of actually tapping each record and generating lineage tags This
  // might be expensive based on the amount of data stored?
  // 5:finalizing RDDs - not to be confused with the actual act of uploading data, this is
  // prepping the data for Ignite to upload. Essentially the materializeBuffers method in the tap
  // rdds.
  // 6: allocating buffers (before any of the tapping occurs).
  // 7 (3/5 edit): properly integrating this into SparkConf for distributed setting.
  
  val PD_TASK_METRICS_ENABLED_KEY = "spark.perfdebug.taskmetrics.enabled"
  lazy val enableSparkContextPerfListenerPrinter: Boolean = {
    val conf = SparkEnv.get.conf
    if(conf.contains(PD_TASK_METRICS_ENABLED_KEY)) {
      val boolStr = conf.get(PD_TASK_METRICS_ENABLED_KEY)
      println(s"Found value for configuration ${PD_TASK_METRICS_ENABLED_KEY}: ${boolStr}. Note " +
                s"that this overrides anything set in PerfDebugConf")
      boolStr.toBoolean
    } else {
      println(s"No value found for configuration ${PD_TASK_METRICS_ENABLED_KEY}. Defaulting to " +
                s"PerfDebugConf value ${_enableSparkContextPerfListenerPrinter}")
      _enableSparkContextPerfListenerPrinter
    }
  }
  
  val PD_USE_SHUFFLE_ESTIMATES = "spark.perfdebug.shufflelatency.enabled"
  lazy val estimateShuffleLatency: Boolean = {
    val conf = SparkEnv.get.conf
    if(conf.contains(PD_USE_SHUFFLE_ESTIMATES)) {
      val boolStr = conf.get(PD_USE_SHUFFLE_ESTIMATES)
      println(s"Found value for configuration ${PD_USE_SHUFFLE_ESTIMATES}: ${boolStr}. Note " +
                s"that this overrides anything set in PerfDebugConf")
      boolStr.toBoolean
    } else {
      println(s"No value found for configuration ${PD_USE_SHUFFLE_ESTIMATES}. Defaulting to " +
                s"PerfDebugConf value ${_estimateShuffleLatency}")
      _estimateShuffleLatency
    }
  }
  
  val PD_IGNITE_LINEAGE_BATCH_SIZE = "spark.perfdebug.lineage.ignite.batchsize"
  lazy val uploadBatchSize: Int = {
    val conf = SparkEnv.get.conf
    if(conf.contains(PD_IGNITE_LINEAGE_BATCH_SIZE)) {
      val confValue = conf.get(PD_IGNITE_LINEAGE_BATCH_SIZE)
      println(s"Found value for configuration ${PD_IGNITE_LINEAGE_BATCH_SIZE}: ${confValue}. Note " +
                s"that this overrides anything set in PerfDebugConf")
      confValue.toInt
    } else {
      println(s"No value found for configuration ${PD_IGNITE_LINEAGE_BATCH_SIZE}. Defaulting to " +
                s"PerfDebugConf value ${_uploadBatchSize}")
      _uploadBatchSize
    }
  }
  
  val PD_PERFQUERY_PRESHUFFLE_TRIM = "spark.perfdebug.lineage.perfquery.preshuffletrim"
  lazy val experimentalPerfQueryFirstShuffleTrim: Option[Int] = {
    val conf = SparkEnv.get.conf
    if(conf.contains(PD_PERFQUERY_PRESHUFFLE_TRIM)) {
      val confValue = conf.get(PD_PERFQUERY_PRESHUFFLE_TRIM)
      println(s"Found value for configuration ${PD_PERFQUERY_PRESHUFFLE_TRIM}: ${confValue}. Note " +
                s"that this overrides anything set in PerfDebugConf")
      val value = confValue.toInt
      if(value > 0) {
        Some(value)
      } else {
        None
      }
    } else {
      println(s"No value found for configuration ${PD_PERFQUERY_PRESHUFFLE_TRIM}. Defaulting to " +
                s"PerfDebugConf value ${_experimentalPerfQueryFirstShuffleTrim}")
      _experimentalPerfQueryFirstShuffleTrim
    }
  }
}

object PerfDebugConf {
  def get: PerfDebugConf = SparkEnv.get.conf.getPerfConf
}