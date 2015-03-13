/**
 * Created by shrinidhihudli on 2/7/15.
 *
 * -- This script covers foreach/generate with a nested distinct.
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *     as (user, action, timespent, query_term, ip_addr, timestamp,
 *         estimated_revenue, page_info, page_links);
 * B = foreach A generate user, action;
 * C = group B by user parallel $PARALLEL;
 * D = foreach C {
 *     aleph = B.action;
 *    beth = distinct aleph;
 *    generate group, COUNT(beth);
 * }
 * store D into '$PIGMIX_OUTPUT/L4out';
 */
package org.apache.spark.examples.sparkmix

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._


object L4 {
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

    lc.setCaptureLineage(lineage)

    val pageViews = lc.textFile(pageViewsPath)

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0),
      SparkMixUtils.safeSplit(x, "\u0001", 1), SparkMixUtils.safeSplit(x, "\u0001", 2),
      SparkMixUtils.safeSplit(x, "\u0001", 3), SparkMixUtils.safeSplit(x, "\u0001", 4),
      SparkMixUtils.safeSplit(x, "\u0001", 5), SparkMixUtils.safeSplit(x, "\u0001", 6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    val B = A.map(x => (x._1, x._2))

    val C = B.groupByKey()

    val D = C.mapValues(_.toSet.size)

    D.collect

    lc.setCaptureLineage(false)

    //    var linRdd = C.getLineage()
    //    linRdd.collect().foreach(println)

    sc.stop()

  }
}