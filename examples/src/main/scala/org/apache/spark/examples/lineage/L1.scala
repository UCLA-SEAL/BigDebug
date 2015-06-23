/**
 * Created by shrinidhihudli on 2/4/15.
 *
 * -- This script tests reading from a map, flattening a bag of maps, and use of bincond.
 *    register $PIGMIX_JAR (register /home/clash/pig-0.14.0/test/perf/pigmix/pigmix.jar)
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

package org.apache.spark.examples.lineage

import org.apache.spark.examples.sparkmix.SparkMixUtils
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.{SparkConf, SparkContext}


object L1 {
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
    conf.setAppName("SparkMix-L1" + lineage + "-" + path)

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

    val B = A.map(x => (x._1, x._2, x._8, x._9)).flatMap(r =>
      for (v <- r._4) yield (r._1, r._2, r._3, v))

    val C = B.map(x =>
      if (x._2 == "1") (x._1, x._3.get("a").toString)
      else (x._1, x._4.get("b").toString)).map(r => (r._1, (r._2, 1L)))

//    val D = C.groupBy(_._1)

//    val E = D.map(x => (x._1, x._2.size))

    val D = C.reduceByKey((a: (String, Long), b: (String, Long)) => (a._1, a._2 + b._2))

    val E = D.map(x => (x._1, x._2._2))

    if(saveToHdfs) {
      E.saveAsTextFile("hdfs://scai01.cs.ucla.edu:9000/clash/lineage/output-L1-"
        + args(1) + "G")

      lc.setCaptureLineage(false)

      Thread.sleep(1000)

        var linRdd = E.getLineage()
        linRdd.collect //.foreach(println)
        //    linRdd.show
        linRdd = linRdd.filter(0)
        linRdd = linRdd.goBackAll()
        linRdd.collect //.foreach(println)
        val value = linRdd.take(1)(0)
        println(value)
        //    sc.unpersistAll(false)
        for(i <- 1 to 10) {
          var linRdd = pageViews.getLineage().filter(r => (r.asInstanceOf[(Any, Int)] == value))
          linRdd.collect()//.foreach(println)
          //    linRdd.show
          linRdd = linRdd.filter(0)

          //    linRdd.collect.foreach(println)
          //    linRdd.show
          linRdd = linRdd.goNextAll()
          linRdd.collect()//.foreach(println)
          println("Done")
        }

    } else {
      E.collect.foreach(println)

      lc.setCaptureLineage(false)

      // Step by step full trace backward
      var linRdd = E.getLineage()
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
      linRdd = E.getLineage()
      linRdd.collect.foreach(println)
      linRdd = linRdd.goBackAll()
      linRdd.collect.foreach(println)
      linRdd.show

      // Step by step trace backward one record
      linRdd = E.getLineage()
      linRdd.collect().foreach(println)
      linRdd = linRdd.filter(3360)
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
      linRdd = E.getLineage()
      linRdd.collect().foreach(println)
      linRdd = linRdd.filter(3360)
      linRdd.collect.foreach(println)
      linRdd = linRdd.goBackAll()
      linRdd.collect.foreach(println)
      linRdd.show

      // Step by step full trace forward
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

      // Step by step full trace forward one record
      linRdd = pageViews.getLineage()
      linRdd.collect.foreach(println)
      linRdd.show
      linRdd = linRdd.filter(2)
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
      linRdd = linRdd.filter(2)
      linRdd.collect.foreach(println)
      linRdd.show
      linRdd = linRdd.goNextAll()
      linRdd.collect.foreach(println)
    }

    sc.stop()
  }

}
