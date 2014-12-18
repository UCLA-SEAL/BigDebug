package org.apache.spark.lineage

import org.apache.spark.lineage.rdd._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Dependency, OneToOneDependency}

import scala.reflect.ClassTag

trait Lineage[T] extends RDD[T] {

  implicit def ttag: ClassTag[T]

  @transient def lineageContext: LineageContext

  def tapRight(): TapLRDD[T] = {
    val tap = new TapLRDD[T](lineageContext,  Seq(new OneToOneDependency(this)))
    setTap(tap)
    setCaptureLineage(true)
    tap
  }

  def tapLeft(): TapLRDD[T] = {
    tapRight()
  }

  def tap(deps: Seq[Dependency[_]]): TapLRDD[T] = {
    val tap = new TapLRDD[T](lineageContext, deps)
    tap.checkpointData = checkpointData
    checkpointData = None
    tap
  }

  def materialize = {
    storageLevel = StorageLevel.MEMORY_ONLY
    this
  }

  protected var tapRDD : Option[TapLRDD[_]] = None

  def setTap(tap: TapLRDD[_] = null) = {
    if(tap == null) {
      tapRDD = None
    } else {
      tapRDD = Some(tap)
    }
  }

  def getTap() = tapRDD

  private[spark] var captureLineage: Boolean = false

  def setCaptureLineage(newLineage :Boolean) = {
    captureLineage = newLineage
    this
  }

  def isLineageActive: Boolean = captureLineage

  def getLineage(): LineageRDD = {
    // Be careful, this can be called from a not TapPostShuffleRDD. Need to add a check
    if(getTap().isDefined) {
      lineageContext.setCurrentLineagePosition(getTap())
      return new LineageRDD(getTap().get.asInstanceOf[Lineage[((Int, Int, Long), Any)]])
    }
    throw new UnsupportedOperationException("no lineage support for this RDD")
  }

  private[spark] def rightJoinLeft(prev: Lineage[((Int, Int, Long), Any)], next: Lineage[((Int, Int, Long), Any)]) = {
    prev.zipPartitions(next) {
      (buildIter, streamIter) =>
        val hashSet = new java.util.HashSet[(Int, Int, Long)]()
        var rowKey: (Int, Int, Long) = null

        // Create a Hash set of buildKeys
        while (buildIter.hasNext) {
          rowKey = buildIter.next()._1
          val keyExists = hashSet.contains(rowKey)
          if (!keyExists) {
            hashSet.add(rowKey)
          }
        }

        streamIter.filter(current => {
          hashSet.contains(current._1)
        })
    }
  }

  private[spark] def rightJoinRight(prev: Lineage[((Int, Int, Long), (Int, Int, Long))], next: Lineage[((Int, Int, Long), Any)]) = {
    prev.zipPartitions(next) {
      (buildIter, streamIter) =>
        val hashSet = new java.util.HashSet[(Int, Int, Long)]()
        var rowKey: (Int, Int, Long) = null

        // Create a Hash set of buildKeys
        while (buildIter.hasNext) {
          rowKey = buildIter.next()._2
          val keyExists = hashSet.contains(rowKey)
          if (!keyExists) {
            hashSet.add(rowKey)
          }
        }

        //hashSet.toArray.foreach(r => println(r.toString))
        streamIter.filter(current => {
          hashSet.contains(current._1)
        })
    }
  }

  /** Returns the first parent Lineage */
  protected[spark] override def firstParent[U: ClassTag]: Lineage[U] = {
    dependencies.head.rdd.asInstanceOf[Lineage[U]]
  }

  /**
   * Return an array that contains all of the elements in this RDD.
   */
  override def collect(): Array[T] = {
    val results = lineageContext.runJob(this, (_: Iterator[T]).toArray)

    if(lineageContext.isLineageActive) {
      lineageContext.setLastLineagePosition(this.getTap())
    }

    Array.concat(results: _*)
  }

  /**
   * Return a new LRDD containing only the elements that satisfy a predicate.
   */
  override def filter(f: T => Boolean): Lineage[T] = {
    if(this.getTap().isDefined) {
      lineageContext.setCurrentLineagePosition(this.getTap())
      var result: ShowRDD = null
      if(this.getTap().get.isInstanceOf[TapPreShuffleLRDD[_]]) {
        val tmp = this.getTap().get.asInstanceOf[TapPreShuffleLRDD[_]]
          .getCachedData.setCaptureLineage(false)
          .asInstanceOf[Lineage[(Any, (Any, (Int, Int, Long)))]]
        tmp.setTap()
        result = new ShowRDD (
          tmp
            .map(r => ((r._1, r._2._1), r._2._2)).asInstanceOf[Lineage[(T, (Int, Int, Long))]]
            .filter(r => f(r._1))
            .map(r => (r._2, r._1.toString()))
        )
        tmp.setTap(lineageContext.getCurrentLineagePosition.get.asInstanceOf[TapLRDD[_]])
      } else if(this.getTap().get.isInstanceOf[TapPostShuffleLRDD[_]]) {
        val tmp = this.getTap().get.asInstanceOf[TapPostShuffleLRDD[_]]
          .getCachedData.setCaptureLineage(false)
          .asInstanceOf[Lineage[(T, (Int, Int, Long))]]
        tmp.setTap()
        result = new ShowRDD (
          tmp
            .filter(r => f(r._1))
            .map(r => (r._2, r._1.toString()))
        )
        tmp.setTap(lineageContext.getCurrentLineagePosition.get.asInstanceOf[TapLRDD[_]])
      } else {
        throw new UnsupportedOperationException
      }
      result.setTap(this.getTap().get)
      return result.asInstanceOf[Lineage[T]]
    } /* else if (this.dependencies(0).rdd.isInstanceOf[TapHadoopLRDD[_, _]]) {
      var result: ShowRDD = null
      lineageContext.setCurrentLineagePosition(Some(this.dependencies(0).rdd.asInstanceOf[TapHadoopLRDD[_, _]]))
      result = new ShowRDD(this.dependencies(0).rdd.asInstanceOf[TapHadoopLRDD[_, _]]
        .firstParent.asInstanceOf[HadoopRDD[LongWritable, Text]]
        .map(r=> (r._1.get(), r._2.toString)).asInstanceOf[RDD[(Long, T)]]
        .filter(r => f(r._2))
        .join(this.dependencies(0).rdd.asInstanceOf[RDD[((Int, Int, Long), (String, Long))]]
        .map(r => (r._2._2, r._1)))
        .distinct()
        .map(r => (r._2._2, r._2._1)).asInstanceOf[RDD[((Int, Int, Long), String)]])
      result.setTap(lineageContext.getCurrentLineagePosition.get.asInstanceOf[TapLRDD[_]])
      return result
    } */
    new FilteredLRDD[T](this, context.clean(f))
  }

  /**
   *  Return a new RDD by first applying a function to all elements of this
   *  RDD, and then flattening the results.
   */
  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): Lineage[U] =
    new FlatMappedLRDD[U, T](this, lineageContext.sparkContext.clean(f))

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  override def map[U: ClassTag](f: T => U): Lineage[U] = new MappedLRDD(this, sparkContext.clean(f))

  override def zipPartitions[B: ClassTag, V: ClassTag]
  (rdd2: RDD[B])
  (f: (Iterator[T], Iterator[B]) => Iterator[V]): Lineage[V] =
    new ZippedPartitionsLRDD2[T, B, V](lineageContext, lineageContext.sparkContext.clean(f), this, rdd2.asInstanceOf[Lineage[B]], false)

  override def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
  (rdd2: RDD[B], rdd3: RDD[C])
  (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): Lineage[V] =
    new ZippedPartitionsLRDD3[T, B, C, V](lineageContext, lineageContext.sparkContext.clean(f), this, rdd2.asInstanceOf[Lineage[B]], rdd3.asInstanceOf[Lineage[C]], false)
}
