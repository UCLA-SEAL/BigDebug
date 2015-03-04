/**
 * Created by shrinidhihudli on 2/5/15.
 *
 * -- This script tests using a join small enough to do in fragment and replicate.
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *   as (user, action, timespent, query_term, ip_addr, timestamp,
 *     estimated_revenue, page_info, page_links);
 * B = foreach A generate user, estimated_revenue;
 * alpha = load '$HDFS_ROOT/power_users' using PigStorage("\u0001") as (name, phone,
 *   address, city, state, zip);
 * beta = foreach alpha generate name;
 * C = join B by user, beta by name using 'replicated' parallel $PARALLEL;
 * store C into '$PIGMIX_OUTPUT/L2out';
 */
package org.apache.spark.examples.sparkmix

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

import org.apache.spark.lineage.LineageContext

object L2 {
  def run(sc: SparkContext, lc: LineageContext, pigMixPath: String, outputPath: String): Long = {

    val properties: Properties = SparkMixUtils.loadPropertiesFile()

    val pageViewsPath = pigMixPath + "page_views/"
    val powerUsersPath = pigMixPath + "power_users"

      val pageViews = lc.textFile(pageViewsPath)
      val powerUsers = lc.textFile(powerUsersPath)
      lc.setCaptureLineage(true)

    val start = System.currentTimeMillis()

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4), SparkMixUtils.safeSplit(x, "\u0001", 5),
      SparkMixUtils.safeSplit(x, "\u0001", 6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    val B = A.map(x => (x._1, x._7))

    val alpha = powerUsers.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4)))

    val beta = alpha.map(x => (x._1, 1))

    val C = B.join(beta).map(x => (x._1, x._2._1, x._1))
    //TODO replicate join

    val end = System.currentTimeMillis()

    C.collect

    if (lc != null)
      lc.setCaptureLineage(false)

    return (end - start)

  }
}
