package org.apache.spark.lineage.perfdebug.lineageV2

import org.apache.spark.lineage.rdd.TapLRDD
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** TapLRDD with a collection of its dependencies which are themselves taps. Used to represent
 * the dependencies between LineageCache instances and is intentionally not exposed. While the
 * RDD is made available, it is primarily there for class matching and is actually near useless
 * outside of the original session as both context and dependencies are lost. */
// private[spark] // jteoh: disabled private on 5/24/2019 for debugging only
case class LineageCacheDependencies(
                                    appId: String, // jteoh: added later to support shuffle agg stats
                                    cacheName: String,
                                    tapName: String,
                                    tapStr: String,
                                    tapRddId: Int,
                                    combinerEnabled: Boolean,
                                    // TODO jteoh: knowing the taps tree might be insufficient for
                                    // performance purposes, eg if we the external storage system
                                    // retains partitioning and an operation such as reduce or
                                    // cogroup does not result in a shuffle, we'll need to
                                    // explicitly store knowledge about the dependency (shuffle
                                    // vs one to one) rather than only the tap instance.
                                    dependencies: Seq[LineageCacheDependencies],
                                    /** To avoid potential issues with post-serialization dependency inspection,
                                     *  also save the number of partitions.
                                     */
                                    numPartitions: Int) {
  
  def this(appId: String, cacheName: String, tap: TapLRDD[_],
           dependencies: Seq[LineageCacheDependencies], numPartitions: Int)  {
    this(appId, cacheName, tap.getClass.getSimpleName, tap.toString, tap.id, tap.isCombinerEnabled,
         dependencies, numPartitions)
  }
  
  /** Uses the [[org.apache.spark.lineage.perfdebug.lineageV2.LineageCacheRepository]] to
   * retrieve the entire lineage cache, without any pre-filtering/joining.
   */
  def fullLineageCache: LineageCache = LineageCacheRepository.getCache(cacheName)
  
  def print(showBefore: Boolean = false): Unit = {
    LineageCacheDependencies.print(this, showBefore)
  }
}

// private[spark] // jteoh: disabled private on 5/24/2019 for debugging only
object LineageCacheDependencies {
  /** Returns a [[LineageCacheDependencies]] that can be used for lineage tracing. In tree
   * terminology, this is essentially the root node.
   * Rather than rewrite the dependencies directly (see
   * [[org.apache.spark.lineage.LineageContext.getLineage()]], which this is based on), this
   * method chooses to create a new structure to store the resulting lineage.
   */
  def buildLineageCacheDependencyTree(lastTap: TapLRDD[_]): LineageCacheDependencies = {
    val result = new mutable.HashMap[TapLRDD[_], ListBuffer[TapLRDD[_]]]
    
    // based on visit in LineageContext.getLineage
    def linkToParentTap(rdd: RDD[_], parent: TapLRDD[_]) {
      val dependencies = new ListBuffer[TapLRDD[_]]()
      
      for (dep <- rdd.dependencies) {
        val newParent: TapLRDD[_] = dep.rdd match {
          case tap: TapLRDD[_] =>
            dependencies.+=:(tap) // prepend bc original code does so, but exclude materialization
            tap
          case _ => parent
        }
        linkToParentTap(dep.rdd, newParent)
      }
      if (dependencies.nonEmpty) {
        val oldDeps: ListBuffer[TapLRDD[_]] =
          result.getOrElseUpdate(parent,
                                 new ListBuffer() ++= parent.dependencies.map(_.rdd)
                                                      .collect({case t: TapLRDD[_] => t}))
        oldDeps ++= dependencies
      }
    }
    
    linkToParentTap(lastTap, lastTap)
    
    val appId = lastTap.context.applicationId
    def convertToLineageCacheDependencies(tap: TapLRDD[_]): LineageCacheDependencies = {
      // assumption: application ID should be the same for all RDDs in dependency tree
      val cacheName = LineageRecordsStorage.getInstance().buildCacheName(appId, tap)
      new LineageCacheDependencies(appId, cacheName, tap,
                               result.getOrElse(tap, Seq.empty).map(convertToLineageCacheDependencies), tap.getNumPartitions)
    }
    
    
    // result.mapValues(_.toSeq).toMap
    // last tap should always have at least one dependency, since there should be an initial
    // data source + some end-of-stage tap
    convertToLineageCacheDependencies(lastTap)
  }
  
  // Variant with depth
  def visit(fn: (LineageCacheDependencies, Int) => Unit, initDepth: Int = 0)
           (current: LineageCacheDependencies): Unit = {
    def visitor(node: LineageCacheDependencies, depth: Int): Unit = {
      fn(node, depth)
      node.dependencies.foreach(visitor(_, depth + 1))
    }
    
    visitor(current, initDepth)
  }
  
  def print(deps: LineageCacheDependencies, showBefore: Boolean = false): Unit = {
    if(showBefore) {
      def visitPrint(rdd: RDD[_], indent: Int = 0): Unit = {
        println("  " * indent + rdd)
        rdd.dependencies.foreach(d => visitPrint(d.rdd, indent + 1))
      }
      // println("----------Debug string----------")
      // println(deps.tap.toDebugString)
      // println("----------Before adjusting dependencies------")
      // visitPrint(deps.tap)
      println("----------Tap dependencies---------")
    }
    visit((node, depth) =>
             println(s"${" " * depth}${node.tapStr}[${node.cacheName}[${node.numPartitions}]]"))(deps)
    if(showBefore) {
      println("----------Done----------")
    }
  }
}