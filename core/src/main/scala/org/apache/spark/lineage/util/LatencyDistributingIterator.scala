package org.apache.spark.lineage.util

import org.apache.spark.{Latency, TaskContext}
import org.apache.spark.lineage.rdd.Lineage

/** Used in flatmap operations - this will compute the latency overhead from the input
 * arguments and distribute them accordingly, if required. Additionally, this iterator
 * measures the time to call cur.next() and finally calls the provided storeFn on the
 * resulting latency. */
case class LatencyDistributingIterator[U] private(storeFn: Latency => Unit, cur: Iterator[U],
                                                  curFromIterator: Boolean, udfTime: Latency,
                                                  iteratorTime: Latency,
                                                  curSize: Option[Int], curSizeTime: Option[Latency])
  extends Iterator[U] {
  // heavily based off iter.flatMap initially, but subject to change later.
  // The flatMap API only requires that the output of the UDF is a TraversableOnce.
  // Based on the scaladocs for [[TraversableOnce]], the two primary traits for this are
  // [[Iterator]] and [[Traversable]].
  // Calculating latency in a flatmap operation can be tricky. For example, consider two
  // possible flatMap UDFs:
  // 1: substring(0, str.indexOf(char)) // upfront computation
  // 2: iterator that returns consecutive characters in string until `char` // iterative
  // Both accomplish the same objective of returning a substring up to a certain point, but
  // their computation overheads vary.
  // Based on this and the understanding of [[TraversableOnce]], I propose the following
  // heuristic measurement:
  // 1. Measure the time for the UDF call (which returns a [[TraversableOnce]]). Call this T_1
  // 2. Check if the [[TraversableOnce]] is a Traversable. If so, measure the time to
  // compute the size and set an appropriate flag (isIterator = false for the docs at least).
  // Call this T_2, and the size CUR_SIZE.
  // 3. Measure the time to call [[TraversableOnce]].toIterator. Call this time T_3. Call
  // this iterator `cur`
  // 4. For each output (this.next()), measure the time to compute cur.next(). Call this
  // T_R. Note that this is per output record, rather than per `cur`/[[TraversableOnce]]
  // like the previous three times.
  // 5. Before returning the output record, compute the following:
  // 5.a: If `cur` came from an [[Iterator]]:
  // 5.a.1: The first record gets time T_R + T_1 + T_3. // no T_2 because not [[Traversable]]
  // 5.a.2: All subsequent records for a given `cur` get time T_R. This resets when a new
  // `cur` is computed (ie flatmap UDF is used again).
  // 5.b: If `cur` came from a [[Traversable]]:
  // 5.b.1: All output records receive time T_R + (T_1 + T_2 + T_3)/CUR_SIZE
  
  // This approach essentially embodies two assumptions:
  // 1. If `cur` is a [[Traversable]], the data is assumed to be computed upfront. As such,
  // the overhead of applying the user UDF should be distributed across all results within
  // `cur.
  // 2. If `cur` is an [[Iterator]], there is no way to determine the overheads. The
  // default assumption is that any overhead of applying the UDF should be attributed to
  // the first record. Ideally we'd distribute over all the records, but there's no way to
  // know how many records there are unless we consume the iterator. If we were to use the
  // size, we would also need to buffer the iterator results which could consume too much
  // memory, hence the 'naive' default assumption.
  // Drawbacks: neither approach is perfect. For example, a buffering iterator
  // in the first situation would result in periodic latency spikes. However, without
  // better insight into the iterator implementation, it is impossible to predict this kind
  // of behavior and know to 'average' latencies over time. Additionally, averaging latencies
  // over time requires buffering because the iterator interface is only traversable once -
  // see the discussion above.
  // Similarly, the Traversable approach tries to make no assumptions other than that all
  // data is computed at once. Out of fairness, it tries to distribute this data evenly.
  // It's perfectly possible for a user to create a Traversable that computes results on
  // the fly but simply saves them along the way (thus being Traversable rather than only
  // TraversableOnce). This 'delayed computation' scenario is not captured by the
  // assumptions made here.
  assert(curFromIterator == curSize.isEmpty,
         "iterator flag should correlate with curSize parameter: " + curSize)
  assert(curFromIterator == curSizeTime.isEmpty,
         "iterator flag should correlate with curSizeTime parameter: " + curSizeTime)
  var firstOutput = true
  // For iterators, the default values should set this up to be udfTime + iteratorTime
  val distributedOverheadTime =
    if(curFromIterator) {
      udfTime + iteratorTime
    } else {
      if (curSize.get != 0) {
        // loss of precision, but this is in ms anyways.
        (udfTime + iteratorTime + curSizeTime.get) / curSize.get
      } else {
        assert(hasNext == false, "curSize cannot be zero if there is a next element")
        0 // doesn't matter - we should never use this.
      }
    }
    
  
  override def hasNext: Boolean = cur.hasNext
  
  override def next(): U = {
    val (record, recordTime) = Lineage.measureTime(cur.next())
    val finalTime =
      if(!firstOutput && curFromIterator) {
        // if we're using an iterator, only the first record receives overhead latency.
        recordTime
      } else {
        distributedOverheadTime + recordTime
      }
    firstOutput = false
    storeFn(finalTime)
    record
  }
}

object LatencyDistributingIterator {
  /** Primary entry point for creating a LatencyStoringIterator. This measures various latencies
   * associated with the input block and distributes them across all results if the block results
   * in a traversable. If it results in an iterator instead, any latency overheads are assigned
   * to the first record.
   */
  def apply[T,U](block: => TraversableOnce[U],
                 taskContext: TaskContext,
                 rddId: Int): LatencyDistributingIterator[U] = {
    import org.apache.spark.lineage.rdd.Lineage._
    var curFromIterator = false
    var curSizeIfTraversable: Option[Int] = None
    var curSizeTime: Option[Latency] = None

    // No need to add a flag here for UDF timing - it's already handled by callers.
    val (udfResult, udfTime) = measureTime(block) // T1: compute, normally the UDF. This uses
    // Scala's call-by-name parameter usage.
    udfResult match {
      case t: Traversable[U] =>
        curFromIterator = false
        val sizeTimeTuple = measureTime(t.size) // T2: size, if applic.
        curSizeIfTraversable = Some(sizeTimeTuple._1)
        curSizeTime = Some(sizeTimeTuple._2)
      case _: Iterator[U] =>
        curFromIterator = true
      case _ =>
        // This should never happen hopefully, but if it does we fallback to the naive
        // iterator approach since there's no guarantee we can rely on the size.
        curFromIterator = true
    }
    val (nextCur, iteratorTime) = measureTime(udfResult.toIterator) //  T3: iterator
    LatencyDistributingIterator(storeContextRecordTime(taskContext, rddId, _),
                           nextCur, curFromIterator,
                           udfTime, iteratorTime,
                           curSizeIfTraversable, curSizeTime)
  }
}