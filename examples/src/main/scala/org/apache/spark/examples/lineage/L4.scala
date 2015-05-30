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
package org.apache.spark.examples.lineage

import org.apache.spark.examples.sparkmix.SparkMixUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._


object L4 {
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
    conf.setAppName("SparkMix-L4" + lineage + "-" + path)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    val pageViewsPath = path + "page_views/"

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

    if(saveToHdfs) {
      D.saveAsTextFile("hdfs://scai01.cs.ucla.edu:9000/clash/lineage/output-L4-" + args(1) + "G")
    } else {
      D.collect.foreach(println)
    }

    lc.setCaptureLineage(false)

    // Step by step full trace backward
    var linRdd = D.getLineage()
    linRdd.collect().foreach(println)
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
    linRdd = D.getLineage()
    linRdd.collect().foreach(println)
    linRdd = linRdd.goBackAll()
    linRdd.collect.foreach(println)
    linRdd.show

    // Step by step trace backward one record
    linRdd = D.getLineage()
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
    linRdd = D.getLineage()
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
