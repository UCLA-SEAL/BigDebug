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
package org.apache.spark.examples.lineage

import org.apache.spark.examples.sparkmix.SparkMixUtils
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}

object L2 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = false
    var saveToHdfs = false
    var path = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/pigmix-spark/pigmix_"
    if(args.size < 2) {
      path = "../../datasets/pigMix/"  + "pigmix_10M/"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      path += args(1) + "G/"
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
      saveToHdfs = true
    }
    conf.setAppName("SparkMix-L2" + lineage + "-" + path)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    val pageViewsPath = path + "page_views/"
    val powerUsersPath = path + "power_users/"

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

    val alpha = powerUsers.map(x =>
      (SparkMixUtils.safeSplit(x, "\u0001", 0), SparkMixUtils.safeSplit(x, "\u0001", 1),
      SparkMixUtils.safeSplit(x, "\u0001", 2), SparkMixUtils.safeSplit(x, "\u0001", 3),
      SparkMixUtils.safeSplit(x, "\u0001", 4)))

    val beta = alpha.map(x => (x._1, 1))

    val C = B.join(beta).map(x => (x._1, x._2._1, x._1))

    if(saveToHdfs) {
      C.saveAsTextFile("hdfs://scai01.cs.ucla.edu:9000/clash/lineage/output-L2-" +
        args(1) + "G")
    } else {
      C.collect.foreach(println)
    }

    lc.setCaptureLineage(false)

    // Step by step full trace backward
    var linRdd = C.getLineage()
    linRdd.collect.foreach(println)
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
    linRdd.show

    // Full trace backward
    linRdd = C.getLineage()
    linRdd.collect.foreach(println)
    linRdd = linRdd.goBackAll()
    linRdd.collect.foreach(println)
    linRdd.show

    // Step by step trace backward one record
    linRdd = C.getLineage()
    linRdd.collect().foreach(println)
    linRdd = linRdd.filter(1)
    linRdd.collect.foreach(println)
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
    linRdd.show

    // Full trace backward one record
    linRdd = C.getLineage()
    linRdd.collect().foreach(println)
    linRdd = linRdd.filter(1)
    linRdd.collect.foreach(println)
    linRdd = linRdd.goBackAll()
    linRdd.collect.foreach(println)
    linRdd.show

    // Step by step trace forward
    linRdd = pageViews.getLineage()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goNext()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goNext()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goNext()
    linRdd.collect.foreach(println)

    // Full trace forward
    linRdd = pageViews.getLineage()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goNextAll()
    linRdd.collect.foreach(println)

    // Step by step trace forward one record
    linRdd = pageViews.getLineage()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.filter(0)
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goNext()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goNext()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goNext()
    linRdd.collect.foreach(println)

    // Full trace forward one record
    linRdd = pageViews.getLineage()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.filter(0)
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goNextAll()
    linRdd.collect.foreach(println)
    sc.stop()
  }
}
