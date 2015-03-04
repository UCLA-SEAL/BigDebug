/**
 * Created by shrinidhihudli on 2/8/15.
 *
 *-- This script covers group all.
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *    as (user, action, timespent, query_term, ip_addr, timestamp,
 *        estimated_revenue, page_info, page_links);
 * B = foreach A generate user, (int)timespent as timespent, (double)estimated_revenue as estimated_revenue;
 * C = group B all;
 * D = foreach C generate SUM(B.timespent), AVG(B.estimated_revenue);
 * store D into '$PIGMIX_OUTPUT/L8out';
 *
 */
package org.apache.spark.examples.sparkmix

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream
import java.io._

import org.apache.spark.lineage.LineageContext

object L8 {
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
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4), SparkMixUtils.safeSplit(x, "\u0001", 5),
      SparkMixUtils.safeSplit(x, "\u0001", 6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    val B = A.map(x => (x._1, SparkMixUtils.safeInt(x._3), SparkMixUtils.safeDouble(x._7)))

    val C = B

    val D = C.reduce((x, y) => ("", x._2 + y._2, x._3 + y._3))

    val E = (D._2, D._3 / C.filter(x => x._3 != 0).count())

    val end = System.currentTimeMillis()

    if (lc != null)
      lc.setCaptureLineage(false)

    val pw = new PrintWriter(new File(outputPath))
    pw.write(E.toString())
    pw.close()

    return (end - start)

  }
}
