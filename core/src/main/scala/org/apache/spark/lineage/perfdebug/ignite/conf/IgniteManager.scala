package org.apache.spark.lineage.perfdebug.ignite.conf

import java.net.InetAddress

import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.spark.internal.Logging


object IgniteManager extends Logging {
  // TODO allow better overrides? This conf does not propagate properly across workers
  private var confPath: Option[String] = None
  private var conf: IgniteConfiguration = new IgniteConfiguration().setClientMode(true)
  
  def getConf: IgniteConfiguration = conf
  
  
  def getIgniteInstance(): Ignite = {
    val hostname = InetAddress.getLocalHost.getHostName
    logInfo(s"Acquiring ignite client from $hostname")
    System.out.println(s"Acquiring ignite client from $hostname")
    val result = Ignition.getOrStart(conf)
    logInfo(s"Finished acquiring ignite client from $hostname")
    System.out.println(s"Finished acquiring ignite client from $hostname")
    result
  }
}
