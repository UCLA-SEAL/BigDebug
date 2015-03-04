/**
 * Created by shrinidhihudli on 2/7/15.
 *
 * --This script does an anti-join.  This is useful because it is a use of
 * --cogroup that is not a regular join.
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *   as (user, action, timespent, query_term, ip_addr, timestamp,
 *       estimated_revenue, page_info, page_links);
 * B = foreach A generate user;
 * alpha = load '$HDFS_ROOT/users' using PigStorage('\u0001') as (name, phone, address,
 *       city, state, zip);
 * beta = foreach alpha generate name;
 * C = cogroup beta by name, B by user parallel $PARALLEL;
 * D = filter C by COUNT(beta) == 0;
 * E = foreach D generate group;
 * store E into '$PIGMIX_OUTPUT/L5out';
 */
package org.apache.spark.examples.sparkmix

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

import org.apache.spark.lineage.LineageContext

object L5 {
  def run(sc: SparkContext, lc: LineageContext, pigMixPath: String, outputPath: String): Long = {

    val properties: Properties = SparkMixUtils.loadPropertiesFile()

    val pageViewsPath = pigMixPath + "page_views/"
    val usersPath = pigMixPath + "users/"

    var pageViews = sc.textFile(pageViewsPath)
    var users = sc.textFile(usersPath)

    if (lc != null) {
      lc.setCaptureLineage(true)
      pageViews = lc.textFile(pageViewsPath)
      users = lc.textFile(usersPath)
    }
    val start = System.currentTimeMillis()


    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4), SparkMixUtils.safeSplit(x, "\u0001", 5),
      SparkMixUtils.safeSplit(x, "\u0001", 6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    val B = A.map(x => (x._1, x._1))

    val alpha = users.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4)))

    val beta = alpha.map(x => (x._1, x._1))

    val C = beta.cogroup(B, properties.getProperty("PARALLEL").toInt)

    val D = C.filter(x => x._2._1.size == 0)

    val E = D.map(_._1)

    val end = System.currentTimeMillis()

    if (lc != null)
      lc.setCaptureLineage(false)

    E.saveAsTextFile(outputPath)

    return (end - start)

  }
}
