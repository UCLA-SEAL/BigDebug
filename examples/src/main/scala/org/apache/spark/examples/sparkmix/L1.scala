/**
 * Created by shrinidhihudli on 2/4/15.
 *
 * -- This script tests reading from a map, flattening a bag of maps, and use of bincond.
 *    register $PIGMIX_JAR
 *    A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *        as (user, action, timespent, query_term, ip_addr, timestamp, estimated_revenue, page_info, page_links);
 *    B = foreach A generate user, (int)action as action, (map[])page_info as page_info,
 *        flatten((bag{tuple(map[])})page_links) as page_links;
 *    C = foreach B generate user,
 *        (action == 1 ? page_info#'a' : page_links#'b') as header;
 *    D = group C by user parallel $PARALLEL;
 *    E = foreach D generate group, COUNT(C) as cnt;
 *    store E into '$PIGMIX_OUTPUT/L1out';
 */

package org.apache.spark.examples.sparkmix


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream

import org.apache.spark.lineage.LineageContext

object L1 {
  def run(sc: SparkContext, lc: LineageContext, pigMixPath: String, outputPath: String): Long = {

    val properties: Properties = SparkMixUtils.loadPropertiesFile()

    val pageViewsPath = pigMixPath + "page_views/"

    var pageViews = sc.textFile(pageViewsPath)

    if (lc != null) {
      pageViews = lc.textFile(pageViewsPath)
      lc.setCaptureLineage(true)
    }

    val start = System.currentTimeMillis()

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4), SparkMixUtils.safeSplit(x, "\u0001", 5),
      SparkMixUtils.safeSplit(x, "\u0001", 6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    val B = A.map(x => (x._1, x._2, x._8, x._9)).flatMap(r => for (v <- r._4) yield (r._1, r._2, r._3, v))

    val C = B.map(x => if (x._2 == "1") (x._1, x._3.get("a").toString) else (x._1, x._4.get("b").toString))

    val D = C.groupBy(_._1) //TODO add $PARALLEL

    val E = D.map(x => (x._1, x._2.size))

    E.collect//saveAsTextFile(outputPath)

    val end = System.currentTimeMillis()

    if (lc != null)
      lc.setCaptureLineage(false)

    return (end - start)

  }

}
