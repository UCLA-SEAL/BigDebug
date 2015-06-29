/**
 * Created by shrinidhihudli on 2/7/15.
 *
 * -- This script covers having a nested plan with splits.
 * register '/usr/local/Cellar/pig/0.14.0/test/perf/pigmix/pigmix.jar'
 * A = load '/user/pig/tests/data/pigmix/page_views' using org.apache.pig.test.pigmix.udf.PigPerformanceLoader()
 *     as (user, action, timespent, query_term, ip_addr, timestamp,
 *        estimated_revenue, page_info, page_links);
 * B = foreach A generate user, timestamp;
 * C = group B by user parallel $PARALLEL;
 * D = foreach C {
 *     morning = filter B by timestamp < 43200;
 *     afternoon = filter B by timestamp >= 43200;
 *     generate group, COUNT(morning), COUNT(afternoon);
 * }
 * store D into '$PIGMIX_OUTPUT/L7out';
 *
 */
package org.apache.spark.examples.lineage

import org.apache.spark.examples.sparkmix.SparkMixUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._


object L7DBDump {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = false
    var saveToHdfs = false
    var path = "hdfs://scai01.cs.ucla.edu:9000/clash/datasets/pigmix-spark/pigmix_"
    var size = ""
    if(args.size < 2) {
      path = "../../datasets/pigMix/"  + "pigmix_10M/"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      path += args(1) + "G/"
      conf.setMaster("spark://SCAI13.CS.UCLA.EDU:7077")
      size = args(1)
      saveToHdfs = true
    }
    conf.setAppName("SparkMix-L7" + lineage + "-" + path)

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

    val B = A.map(x => (x._1, SparkMixUtils.safeInt(x._6)))

    val C = B.groupByKey()

    val D = C.mapValues(x => x.map(y => if (y < 43200) "morning" else "afternoon"))
      .map(x => (x._1, x._2.groupBy(identity))).map(x => (x._1, x._2.mapValues(x => x.size).map(identity)))

    if(saveToHdfs) {
      D.saveAsTextFile("hdfs://scai01.cs.ucla.edu:9000/clash/lineage/output-L7-" + args(1) + "G")

      lc.setCaptureLineage(false)

      Thread.sleep(10000)
    } else {
      D.collect.foreach(println)


      lc.setCaptureLineage(false)
    }

    var linRdd = D.getLineage()
    linRdd.saveAsCSVFile("tap-" + size)
    linRdd = C.getLineage()
    linRdd.saveAsCSVFile("post-" + size)
    linRdd = linRdd.getBack()
    linRdd.saveAsCSVFile("pre-" + size)
    linRdd = pageViews.getLineage()
    linRdd.saveAsCSVFile("hadoop-" + size)

    sc.stop()
  }
}
