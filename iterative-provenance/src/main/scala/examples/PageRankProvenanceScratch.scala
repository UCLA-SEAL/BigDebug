package examples

import org.apache.spark.SparkConf
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.sql.SparkSession

object ScratchRDD extends App {
  val iters = 3
  
  
  val conf = new SparkConf().setMaster("local[*]").set("spark.eventLog.dir", "/tmp/spark-events")
                            .set("spark.eventLog.enabled", "true")
  val spark = SparkSession
    .builder.config(conf)
    .appName("examples.ScratchRDD")
    .getOrCreate()
  
  spark.sparkContext.setLogLevel("WARN")
  //  spark.sparkContext.setLogLevel("DEBUG")
  val sc = spark.sparkContext
  val lc = new LineageContext(sc)
  lc.setCaptureLineage(true)
  val file = args.lift(0).getOrElse("/Users/jteoh/Code/FineGrainedDataProvenance/testing_graph_pr" +
                                      ".txt")
  val lines = lc.textFile(file)
  val links = lines.map{ s =>
    val parts = s.split("\\s+")
    (parts(0), parts(1))
  }.distinct().groupByKey().cache()
  
  var ranks = links.mapValues(v => 1.0).setName("Ranks @ iteration 0")
  
  for (i <- 1 to iters) {
    // scalastyle:off println
    println(s"==========Iteration $i=========")
    // scalastyle:on println
    val oldRanks = ranks
    val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
      val size = urls.size
      urls.map(url => (url, rank / size))
    }
    
    
    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
                    .setName(s"Ranks @ iteration $i").cache()
    ranks.count()
    oldRanks.unpersist(blocking = false)
  }
  
  ranks.collect()
  val temp: Lineage[_] = lc.getLineage(ranks)
  
  val inputs = temp.mapPartitions(iter => Iterator(iter.next())).collect()
  println(inputs.mkString(","))
}