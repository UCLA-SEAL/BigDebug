package org.apache.spark.lineage.perfdebug.ignite.conf

import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.configuration.IgniteConfiguration


object IgniteManager {
  // TODO allow better overrides? This conf does not propagate properly across workers
  private var conf: IgniteConfiguration = new IgniteConfiguration().setClientMode(true)
  
  def setConf(conf: IgniteConfiguration) = {
    this.conf = conf
    throw new UnsupportedOperationException("Setting Ignite confs isn't supported due to improper" +
                                              " worker propagation.")
  }
  
  def getConf: IgniteConfiguration = conf
  
  
  def getIgniteInstance(): Ignite = {
    Ignition.getOrStart(conf)
  }
}
