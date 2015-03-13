/**
 * Created by shrinidhihudli on 2/6/15.
 *
 * --This script tests a join too large for fragment and replicate.  It also
 * --contains a join followed by a group by on the same key, something that we
 * --could potentially optimize by not regrouping.
 * register $PIGMIX_JAR
 * A = load '$HDFS_ROOT/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *   as (user, action, timespent, query_term, ip_addr, timestamp,
 *       estimated_revenue, page_info, page_links);
 * B = foreach A generate user, (double)estimated_revenue;
 * alpha = load '$HDFS_ROOT/users' using PigStorage('\u0001') as (name, phone, address,
 *       city, state, zip);
 * beta = foreach alpha generate name;
 * C = join beta by name, B by user parallel $PARALLEL;
 * D = group C by $0 parallel $PARALLEL;
 * E = foreach D generate group, SUM(C.estimated_revenue);
 * store E into '$PIGMIX_OUTPUT/L3out';
 */
package org.apache.spark.examples.sparkmix

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

object L3 {
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
    val usersPath = pigMixPath + "users/"

    lc.setCaptureLineage(lineage)

    val pageViews = lc.textFile(pageViewsPath)
    val users = lc.textFile(usersPath)

    val A = pageViews.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), 
      SparkMixUtils.safeSplit(x, "\u0001", 1), SparkMixUtils.safeSplit(x, "\u0001", 2), 
      SparkMixUtils.safeSplit(x, "\u0001", 3), SparkMixUtils.safeSplit(x, "\u0001", 4), 
      SparkMixUtils.safeSplit(x, "\u0001", 5), SparkMixUtils.safeSplit(x, "\u0001", 6),
      SparkMixUtils.createMap(SparkMixUtils.safeSplit(x, "\u0001", 7)),
      SparkMixUtils.createBag(SparkMixUtils.safeSplit(x, "\u0001", 8))))

    val B = A.map(x => (x._1, SparkMixUtils.safeDouble(x._7)))

    val alpha = users.map(x => (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4)))

    val beta = alpha.map(x => (x._1, 1))

    val C = B.join(beta)//.map(x => (x._1, x._1, x._2._1))
    C.collect

    val D = C.map(x => (x._1, x._1, x._2._1)).groupBy(_._1) //TODO add $PARALLEL

    D.collect
    val E = D.map(x => (x._1, x._2.reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3)))).map(x => (x._1, x._2._3))

    E.collect

    lc.setCaptureLineage(false)

    //    var linRdd = C.getLineage()
    //    linRdd.collect().foreach(println)

    sc.stop()
  }
}
