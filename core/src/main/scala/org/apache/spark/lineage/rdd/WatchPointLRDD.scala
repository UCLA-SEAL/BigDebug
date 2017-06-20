package org.apache.spark.lineage.rdd

import org.apache.spark.bdd._
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.{Partition, TaskContext, TaskKilledException}

import scala.collection.Iterator._
import scala.reflect._

/**
 * Rewriting watchpoint class with MapPartitionRDD. -- Tag Bigdebug @ Gulzar 6/16
 */

 class WatchPointLRDD[U: ClassTag, T: ClassTag](prev: Lineage[T],
                                                f: (TaskContext, Int, Iterator[T]) => Iterator[U],  // (TaskContext, partition index, iterator)
                                                preservesPartitioning: Boolean = false)
     extends MapPartitionsLRDD[U, T](prev, f, preservesPartitioning) with Lineage[U] {

  override def lineageContext = prev.lineageContext

  override def ttag = classTag[U]
  
  /**
   * Re Compile the new filter at driver and sends it to the workers
   * Call only at Driver
   * */
  override def reCompileFilter(code:String): String  ={
    val compiler = new BDDPredicateCompiler1(None)
    val  pc:BDDFilter[T] = compiler.eval[BDDFilter[T]](code)
    pc.getClass.getName
  }
}


class WatchpointInterruptableIterator[T](val context: TaskContext, val delegate: Iterator[T], rddid :Int )
  extends Iterator[T] {
  self =>


  override def filter(p: T => Boolean): Iterator[T] = new BDDAbstractIterator[T] {

    var func: T => Boolean = null

    /** Load the received filter class at the Worker */
    var filterflip = -1

    def loadFilter() = {
      val cls = WatchpointManager.getFilterClass(self.rddid, filterflip)
      if (cls != null) {
        filterflip = filterflip + 1
        val filterclass: BDDFilter[T] = cls.getConstructor().newInstance().asInstanceOf[BDDFilter[T]]
        func = filterclass.function
      } else {}
    }

    def truePredicate(value: T, p: T => Boolean): Boolean = {
      loadFilter()
      if (func != null) {
        if (func(value)) {
     //     println("Tapped Record func :  " + value.toString)
          WatchpointManager.captureWatchpointRecord(value.toString, context.partitionId, self.rddid)
        }} else {
          if (p(value)) {
         //   println("Tapped Record P :  " + value.toString)
            WatchpointManager.captureWatchpointRecord(value.toString, context.partitionId, self.rddid)

          }
        }
        return true
      }

      private var hd: T = _
      private var hdDefined: Boolean = false

      def hasNext: Boolean = hdDefined || {
        do {
          if (!self.hasNext) return false
          hd = self.next()
        } while (!truePredicate(hd, p))
        hdDefined = true
        true
      }

      def next() = if (hasNext) {
        hdDefined = false; hd
      } else empty.next()
    }

    def hasNext: Boolean = {
      if (context.isInterrupted) {
        throw new TaskKilledException
      } else{
          val a=delegate.hasNext
        if(!a) WatchpointManager.setTaskDone(context.partitionId, self.rddid, context)
        a
    }
    }

    def next(): T = {
      delegate.next()
    }

  }
