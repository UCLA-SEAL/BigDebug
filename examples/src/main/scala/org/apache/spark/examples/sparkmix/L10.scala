/**
 * Created by shrinidhihudli on 2/8/15.
 *
 * --This script covers order by of multiple values.
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *     as (user, action, timespent:int, query_term, ip_addr, timestamp,
 *         estimated_revenue:double, page_info, page_links);
 * B = order A by query_term, estimated_revenue desc, timespent parallel $PARALLEL;
 * store B into '$PIGMIX_OUTPUT/L10out';
 *
 */
package org.apache.spark.examples.sparkmix

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

import org.apache.spark.lineage.LineageContext

object L10 {
  def run(sc: SparkContext, lc: LineageContext, pigMixPath: String, outputPath: String): Long = {

    val properties: Properties = SparkMixUtils.loadPropertiesFile()

    val pageViewsPath = pigMixPath + "page_views/"

    var pageViews = sc.textFile(pageViewsPath)

    if (lc != null) {
      lc.setCaptureLineage(true)
      pageViews = lc.textFile(pageViewsPath)
    }

    val start = System.currentTimeMillis()

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeInt(SparkMixUtils.safeSplit(x, "\u0001", 2)), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4), SparkMixUtils.safeSplit(x, "\u0001", 5),
      SparkMixUtils.safeDouble(SparkMixUtils.safeSplit(x, "\u0001", 6)),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    //val B = A.sortBy(_._4, true, properties.getProperty("PARALLEL").toInt).
    //  sortBy(_._7, false, properties.getProperty("PARALLEL").toInt).
    //  sortBy(_._3, true, properties.getProperty("PARALLEL").toInt)

    val B = A.sortBy(r => (r._4, r._7, r._3),true,properties.getProperty("PARALLEL").toInt)

    val end = System.currentTimeMillis()

    if (lc != null)
      lc.setCaptureLineage(false)

    B.saveAsTextFile(outputPath)

    return (end - start)

  }
}
