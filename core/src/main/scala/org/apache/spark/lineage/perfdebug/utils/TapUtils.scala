package org.apache.spark.lineage.perfdebug.utils

import org.apache.spark.lineage.rdd._

/** Collection of helper methods to identify TapRDD instances. In practice these are better as
 * traits. However, TapLRDD is a base class while some of its behaviors should be overriden -
 * there's no way (I know of) to override traits and refactoring is dangerous with this legacy
 * code, so we define a new set of functions to identify the classes accordingly.
 *
 * Note that this is not an exhaustive list. For example, the shuffles have a pre/post shuffle
 * latency tap trait that is leveraged instead.
 * @author jteoh
 */
object TapUtils {
  implicit class TapWrapper(tap: TapLRDD[_]) {
    def isEndOfStageTap: Boolean = {
      isPreShuffle || isTapLRDD
    }
  
    def isStartOfStageTap: Boolean = {
      !isEndOfStageTap // we never tap in between.
      // This technically includes isSourceTap, though not explicitly written.
    }
  
    def isShuffleAggTap: Boolean = {
      tap match {
        case _: LatencyStatsTap[_] => true
        case _ => false
      }
    }
  
    def hasMultipleInputs: Boolean = {
      isShuffleAggTap // 8/21/2018 jteoh: these are equivalent.
    }
    
    def isSourceTap: Boolean = {
      tap match {
        case _: TapHadoopLRDD[_,_] | _: TapParallelCollectionLRDD[_] => true
        case _ => false
      }
    }
    
    def hasPredecessor: Boolean = {
      // This should technically be figured out via lineage dependencies, but it's included here
      // for convenience too.
      !isSourceTap
    }
  
    private def isPostShuffle: Boolean = {
      tap match {
        case _: PostShuffleLatencyStatsTap[_] => true
        case _ => false
      }
    }
  
    private def isPreShuffle: Boolean = {
      tap match {
        case _: PreShuffleLatencyStatsTap[_] => true
        case _ => false
      }
    }
  
    // Unfortunately TapLRDD is a class rather than a trait, but there's too much legacy code to
    // try separating out a base implementation. Here's an ugly workaround.
    private def isTapLRDD: Boolean = {
      tap.getClass.equals(classOf[TapLRDD[_]])
    }
  }
  
  /** Quick fix to support removal of tap within lineage cache deps */
  implicit class TapNameWrapper(tapName: String) {
    def isEndOfStageTap: Boolean = {
      isPreShuffle || isTapLRDD
    }
    
    def isStartOfStageTap: Boolean = {
      !isEndOfStageTap // we never tap in between.
      // This technically includes isSourceTap, though not explicitly written.
    }
    
    def isShuffleAggTap: Boolean = {
      tapName == classOf[LatencyStatsTap[_]].getSimpleName
    }
    
    def hasMultipleInputs: Boolean = {
      isShuffleAggTap // 8/21/2018 jteoh: these are equivalent.
    }
    
    def isSourceTap: Boolean = {
       isHadoopTap ||
        tapName == classOf[TapParallelCollectionLRDD[_]].getSimpleName
    }
    
    def isHadoopTap: Boolean = {
      tapName == classOf[TapHadoopLRDD[_, _]].getSimpleName
    }
    
    def hasPredecessor: Boolean = {
      // This should technically be figured out via lineage dependencies, but it's included here
      // for convenience too.
      !isSourceTap
    }
    
    private def isPostShuffle: Boolean = {
      tapName ==  classOf[PostShuffleLatencyStatsTap[_]].getSimpleName
    }
    
    private def isPreShuffle: Boolean = {
      tapName == classOf[PreShuffleLatencyStatsTap[_]].getSimpleName
    }
    
    def isLatencyStatsTap: Boolean = {
      isPostShuffle || isPreShuffle // TODO: cogroups
    }
    
    // Unfortunately TapLRDD is a class rather than a trait, but there's too much legacy code to
    // try separating out a base implementation. Here's an ugly workaround.
    private def isTapLRDD: Boolean = {
      tapName == classOf[TapLRDD[_]].getSimpleName
    }
  }
}

