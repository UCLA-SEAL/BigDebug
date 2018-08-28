package org.apache.spark.lineage.perfdebug.utils

import org.apache.spark.lineage.perfdebug.lineageV2.LineageWrapper._
import org.apache.spark.lineage.perfdebug.utils.CacheDataTypes._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD

object PerfLineageUtils {
  
  /** Join backwards in data lineage until the original data source (or first/leftmost), then
   * join to the raw hadoop data. Print and return RDD.
   *
   * Key parts that are still required and not externalized:
   * Execution graph (how the caches relate to each other)
   * raw data RDDs
   */
  def traceBackAndPrint(rdd: Lineage[_]): RDD[(Long, String)] = {
    val lastLineageWrapper = rdd.lineageWrapper
    println("----------Tap dependencies---------")
    lastLineageWrapper.printDependencies() // debugging
    
    // trace to the data source (hadoop)
    val hadoopLineageWrapper = lastLineageWrapper.traceBackAll()
    val rawHadoopValues = hadoopLineageWrapper.rawInputRDD
    
    printRDDWithMessage(rawHadoopValues, "Raw input data that was traced (with byte offsets):")
    rawHadoopValues
  }
  
  
  
  /* Utility method to print a list of cache values with their schema. By default, will print
  out the count at the end.
   */
  def printCacheValues[V <: CacheValue](values: Traversable[V], withCount: Boolean = true): Unit = {
    if (values.nonEmpty) {
      val schema = values.head match {
        case _: TapLRDDValue => TapLRDDValue.readableSchema
        case _: TapHadoopLRDDValue => TapHadoopLRDDValue.readableSchema
        case _: TapPreShuffleLRDDValue => TapPreShuffleLRDDValue.readableSchema
        case _: TapPostShuffleLRDDValue => TapPostShuffleLRDDValue.readableSchema
        case _ => "WARNING: Undefined schema"
      }
      println(schema)
    }
    values.foreach(v => println("\t" + v))
    println("Printed count: " + values.size)
  }
  
  def printRDDWithMessage(rdd: RDD[_], msg: String, printSeparatorLines: Boolean = true, limit:
  Option[Int] = None, cacheRDD: Boolean = false): Unit = {
    // TODO WARNING - ENABLING CACHE FLAG REALLY SCREWS THINGS UP FOR UNKNOWN REASONS
    
    // The general assumption is the input RDD will also be used later, so cache it pre-emptively
    if(cacheRDD) rdd.cache()
    // collect output before printing because of console output
    val values = if(limit.isDefined) {
      //rdd.take(limit.get) // not supported in lineage :(
      
      rdd.collect().take(limit.get) // yikes
    } else {
      rdd.collect()
    }
    val numResults = values.length
    val sepChar = "-"
    val sepStr = sepChar * 100
    
    val resultCountStr =
      limit.filter(_ == numResults).map("Up to " + _).getOrElse("All " + numResults) + " result(s) shown"
    if(printSeparatorLines) println(sepStr)
    
    println(msg)
    values.foreach(s => println("\t" + s))
    println(resultCountStr)
    if(printSeparatorLines) println(sepStr)
    
  }
}
