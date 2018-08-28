package org.apache.spark.lineage.perfdebug.perftrace

import org.apache.spark.lineage.perfdebug.storage.PerfLineageCacheStorage
import org.apache.spark.lineage.rdd.TapLRDD
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** TapLRDD with a collection of its dependencies which are themselves taps. Used to represent
 * the dependencies between LineageCache instances and is intentionally not exposed. While the
 * RDD is made available, it is primarily there for class matching and is actually near useless
 * outside of the original session as both context and dependencies are lost. */
private[spark]
case class LineageCacheDependencies(appId: String, // jteoh: added later to support shuffle agg stats
                                    cacheName: String,
                                    tap: TapLRDD[_], //TODO jteoh: consider changing to string -
                                    // this isn't storage-friendly.
                                    dependencies: Seq[LineageCacheDependencies]) {
  
  /** Uses the [[org.apache.spark.lineage.perfdebug.perftrace.LineageCacheRepository]] to
   * retrieve the entire lineage cache, without any pre-filtering/joining.
   */
  def fullLineageCache: LineageCache = LineageCacheRepository.getCache(cacheName)
  
  def print(showBefore: Boolean = false): Unit = {
    LineageCacheDependencies.print(this, showBefore)
  }
}

private[spark]
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
      val cacheName = PerfLineageCacheStorage.getInstance().buildCacheName(appId, tap)
      LineageCacheDependencies(appId, cacheName, tap,
                               result.getOrElse(tap, Seq.empty).map(convertToLineageCacheDependencies))
    }
    
    
    // result.mapValues(_.toSeq).toMap
    // last tap should always have at least one dependency, since there should be an initial
    // data source + some end-of-stage tap
    convertToLineageCacheDependencies(lastTap)
  }
  
  // Variant with depth
  def visit(fn: (String, TapLRDD[_], Int) => Unit, initDepth: Int = 0)
           (current: LineageCacheDependencies): Unit = {
    def visitor(node: LineageCacheDependencies, depth: Int): Unit = {
      fn(node.cacheName, node.tap, depth)
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
      println("----------Debug string----------")
      println(deps.tap.toDebugString)
      println("----------Before adjusting dependencies------")
      visitPrint(deps.tap)
      println("----------Tap dependencies---------")
    }
    visit((cacheName, tapRDD, depth) => println(" " * depth + tapRDD + s"[$cacheName]"))(deps)
    if(showBefore) {
      println("----------Done----------")
    }
  }
  
}