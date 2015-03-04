/**
 * Created by shrinidhihudli on 2/9/15.
 *
 * -- This script covers distinct and union.
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *     as (user, action, timespent, query_term, ip_addr, timestamp,
 *         estimated_revenue, page_info, page_links);
 * B = foreach A generate user;
 * C = distinct B parallel $PARALLEL;
 * alpha = load '$HDFS_ROOT/widerow' using PigStorage('\u0001');
 * beta = foreach alpha generate $0 as name;
 * gamma = distinct beta parallel $PARALLEL;
 * D = union C, gamma;
 * E = distinct D parallel $PARALLEL;
 * store E into '$PIGMIX_OUTPUT/L11out';
 *
 */
package org.apache.spark.examples.sparkmix

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

import org.apache.spark.lineage.LineageContext

object L11 {
  def run(sc: SparkContext, lc: LineageContext, pigMixPath: String, outputPath: String): Long = {

    val properties: Properties = SparkMixUtils.loadPropertiesFile()

    val pageViewsPath = pigMixPath + "page_views/"
    val widerowPath = pigMixPath + "widerow/"


    var pageViews = sc.textFile(pageViewsPath)
    var alpha = sc.textFile(widerowPath)

    if (lc != null) {
      lc.setCaptureLineage(true)
      pageViews = lc.textFile(pageViewsPath)
      alpha = lc.textFile(widerowPath)
    }

      val start = System.currentTimeMillis()

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4), SparkMixUtils.safeSplit(x, "\u0001", 5),
      SparkMixUtils.safeSplit(x, "\u0001", 6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    val B = A.map(_._1)

    val C = B.distinct(properties.getProperty("PARALLEL").toInt)



    val beta = alpha.map(x => x.split("\u0001")(0))

    val gamma = beta.distinct(properties.getProperty("PARALLEL").toInt)

    val D = C.union(gamma)

    val E = D.distinct(properties.getProperty("PARALLEL").toInt)

    val end = System.currentTimeMillis()

    if (lc != null)
      lc.setCaptureLineage(false)

    E.saveAsTextFile(outputPath)

    return (end - start)
  }
}
