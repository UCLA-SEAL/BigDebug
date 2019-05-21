package org.apache.spark.lineage.perfdebug.ignite.conf

import java.net.InetAddress
import java.util

import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder
import org.apache.spark.internal.Logging


object IgniteManager extends Logging {
  // TODO allow better overrides? This conf does not propagate properly across workers
  private var confPath: Option[String] = None
  private var conf: IgniteConfiguration = {
    val cfg = new IgniteConfiguration().setClientMode(true)
    val spi = new TcpDiscoverySpi()
    val ipFinder = new TcpDiscoveryMulticastIpFinder()
    // use default multicast group in addition to static ips, though it probably won't work.
    // TODO see if there's a way to sync this to cluster
    ipFinder.setAddresses(util.Arrays.asList("131.179.96.203", "131.179.96.204", "131.179.96.205", "131.179.96.206", "131.179.96.207"));
    // configs
    spi.setIpFinder(ipFinder)
    cfg.setDiscoverySpi(spi)
    cfg
  }
  
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
