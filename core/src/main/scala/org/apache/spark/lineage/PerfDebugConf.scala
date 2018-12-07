package org.apache.spark.lineage

import org.apache.spark.SparkEnv

/**
 * Primitive configuration class for storing various toggles. This is currently intended for use
 * only on the driver side (i.e., worker classes should be configured beforehand).
 * Created 11/26/2018
 *
 * @author jteoh
 */
case class PerfDebugConf(wrapUDFs: Boolean = true,
    // TODO look for "shuffle flag (instrumentation toggle)" TODOs and complete them.
                         estimateShuffleLatency: Boolean = true,
                         uploadLineage: Boolean = true,
                         uploadLineageRecordsLimit: Int = -1,
                         uploadBatchSize: Int = 100000,
                         tapRDDs: Boolean = true,
                         materializeBuffers: Boolean = true,
                         allocateBuffers: Boolean = true,
                         uploadIgniteDataAfterConversion: Boolean = false) extends Serializable {
  
  if(tapRDDs && !allocateBuffers) {
    println("Warning: RDD lineage buffers are not allocated but tapping is still enabled - these " +
              "tags will not be stored")
  }
  if(materializeBuffers && !allocateBuffers) {
    println("Warning: RDD lineage buffers are not allocated but materialization is still enabled.")
  }
  
  if(uploadLineage && !materializeBuffers) {
    println("Warning: RDD lineage buffers are not materialized, but empty data is still uploaded " +
              "to ignite.")
  }
  
  if(uploadLineageRecordsLimit > 0 && !uploadLineage) {
    println("Warning: RDD lineage upload limit is ignored because lineage uploading is disabled.")
  }
  
  if(uploadIgniteDataAfterConversion) {
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
  
}

object PerfDebugConf {
  def get: PerfDebugConf = SparkEnv.get.conf.getPerfConf
}