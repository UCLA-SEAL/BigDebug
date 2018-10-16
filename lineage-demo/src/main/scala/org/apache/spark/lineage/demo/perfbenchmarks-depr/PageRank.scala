package org.apache.spark.lineage.demo.perfbenchmarks

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.demo.LineageBaseApp
import org.apache.spark.lineage.perfdebug.lineageV2.LineageCacheRepository
import org.apache.spark.lineage.rdd.Lineage

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 *
 * Example Usage:
 * {{{
 * bin/run-example SparkPageRank data/mllib/pagerank_data.txt 10
 * }}}
 */
object PageRank extends LineageBaseApp() {
  
  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }
  
  override def run(lc: LineageContext, args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.err.println("Perhaps you want to use " +
                           "/Users/jteoh/Code/Performance-Debug-Benchmarks/PageRank/pagerank_data" +
                           ".txt ?")
      System.exit(1)
    }
    
    showWarning()
    //val logFile = "C:/Program Files/spark-2.3.1-bin-hadoop2.7/README.md" // Should be some file on your system
    
    //val logData = sc.textFile(logFile, 2)
    
    
    //    val spark = SparkSession
    ////      .builder
    ////      .appName("SparkPageRank")
    ////      .getOrCreate()
    
    val iters = if (args.length > 1) args(1).toInt else 10
    val file = args(0)
    println(s"Running page rank for $iters iterations on $file")
    val lines = lc.textFile(args(0), 2)
    //val lines = spark.read.textFile(args(0)).rdd
    val links: Lineage[(String, Iterable[String])] = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    
    var ranks: Lineage[(String, Double)] = links.mapValues(v => 1.0)
    
    
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
        
      }
      // TODO  - SOME UNKNOWN ERROR HAPPENS HERE.
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
    println(ranks.toDebugString)
    
    try {
      // Bug here - cogroup isn't being tapped properly
      val output = ranks.collect()
      output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
    } catch {
      case _: Exception =>
        println("POST-TAP")
        println(ranks.toDebugString)
    }
    
  
  
    //val rankLineage = ranks //ranks.asInstanceOf[Lineage[(String, Double)]]
    //val rankLineageRDD = rankLineage.getLineage()
    //println(rankLineageRDD.toDebugString)
    // val wrapper = rankLineage.lineageWrapper

    LineageCacheRepository.close()
  }
  
}
