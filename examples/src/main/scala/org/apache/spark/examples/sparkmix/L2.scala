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

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}

object L2 {
  def main(args: Array[String]) {

    val dataSize = args(0)
    val lineage: Boolean = args(1).toBoolean

    val pigMixPath = "../../datasets/pigMix/"  + "pigmix_" + dataSize + "/"

    val conf = new SparkConf()
      .setAppName("SparkMix")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    val pageViewsPath = pigMixPath + "page_views/"
    val powerUsersPath = pigMixPath + "power_users"

    lc.setCaptureLineage(lineage)
    val pageViews = lc.textFile(pageViewsPath)
    val powerUsers = lc.textFile(powerUsersPath)

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), 
      SparkMixUtils.safeSplit(x, "\u0001", 1), SparkMixUtils.safeSplit(x, "\u0001", 2), 
      SparkMixUtils.safeSplit(x, "\u0001", 3), SparkMixUtils.safeSplit(x, "\u0001", 4), 
      SparkMixUtils.safeSplit(x, "\u0001", 5), SparkMixUtils.safeSplit(x, "\u0001", 6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    val B = A.map(x => (x._1, x._7))

    val alpha = powerUsers.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4)))

    val beta = alpha.map(x => (x._1, 1))

    val C = B.join(beta).map(x => (x._1, x._2._1, x._1))
    //TODO replicate join

    C.collect

    lc.setCaptureLineage(false)

//    var linRdd = C.getLineage()
//    linRdd.collect().foreach(println)

    sc.stop()
  }
}
