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
import java.io.{File, FileInputStream}

import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.LineageContext


object L1 {
  def main(args: Array[String]) {

    val properties = SparkMixUtils.loadPropertiesFile()
    val dataSize = args(0)
    val lineage: Boolean = args(1).toBoolean

    val pigMixPath = properties.getProperty("pigMix") + "pigmix_" + dataSize + "/"
    val outputRoot = properties.getProperty("output") + "pigmix_" + dataSize + "_" + (System.currentTimeMillis() / 100000 % 1000000) + "/"

    new File(outputRoot).mkdir()

    val conf = new SparkConf().setAppName("SparkMix").setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    val pageViewsPath = pigMixPath + "page_views/"
    val pageViews = lc.textFile(pageViewsPath)

    lc.setCaptureLineage(lineage)

    val start = System.currentTimeMillis()

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0),
      SparkMixUtils.safeSplit(x, "\u0001", 1), SparkMixUtils.safeSplit(x, "\u0001", 2),
      SparkMixUtils.safeSplit(x, "\u0001", 3), SparkMixUtils.safeSplit(x, "\u0001", 4), 
      SparkMixUtils.safeSplit(x, "\u0001", 5), SparkMixUtils.safeSplit(x, "\u0001", 6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    val B = A.map(x => (x._1, x._2, x._8, x._9)).flatMap(r => for (v <- r._4) yield (r._1, r._2, r._3, v))

    val C = B.map(x => if (x._2 == "1") (x._1, x._3.get("a").toString) else (x._1, x._4.get("b").toString))

    val D = C.groupBy(_._1) //TODO add $PARALLEL

    val E = D.map(x => (x._1, x._2.size))

    val end = System.currentTimeMillis()

    E.collect//saveAsTextFile(outputPath)

    lc.setCaptureLineage(false)

    println(end - start)

    sc.stop()
  }

}
