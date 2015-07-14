package org.apache.spark.lineage.rdd

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.Partitioner._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.{OneToOneDependency, Partitioner}

import scala.language.implicitConversions
import scala.reflect.ClassTag

trait Lineage[T] extends RDD[T] {

  implicit def ttag: ClassTag[T]

  @transient def lineageContext: LineageContext

  protected var tapRDD : Option[TapLRDD[_]] = None

  // None = no cache, true = pre, false = post
  private[spark] var isPreShuffleCache: Option[Boolean] = None

  def tapRight(): TapLRDD[T] = {
    val tap = new TapLRDD[T](lineageContext,  Seq(new OneToOneDependency(this)))
    setTap(tap)
    setCaptureLineage(true)
    tap
  }

  def tapLeft(): TapLRDD[T] = tapRight()

  def materialize = {
    storageLevel = StorageLevel.MEMORY_ONLY
    this
  }

  def setTap(tap: TapLRDD[_] = null) = {
    if(tap == null) {
      tapRDD = None
    } else {
      tapRDD = Some(tap)
    }
    this
  }

  def getTap = tapRDD

  def setCaptureLineage(newLineage :Boolean) = {
    captureLineage = newLineage
    this
  }

  def getLineage(): LineageRDD = {
    if(getTap.isDefined) {
      lineageContext.setCurrentLineagePosition(getTap)
      return getTap.get match {
        case _: TapPostShuffleLRDD[_] | _: TapPreShuffleLRDD[_] =>
          getTap.get
        case tap: TapHadoopLRDD[Any @unchecked, Long @unchecked] =>
          tap.map(_.swap)
        case tap: TapLRDD[(Int, Int) @unchecked] =>
          tap.map(r => ((0L, r._1), (0L, r._2)))
      }
    }
    throw new UnsupportedOperationException("no lineage support for this RDD")
  }

  def setIsPreShuffleCache(): Lineage[T] = {
    this.isPreShuffleCache = Some(true)
    this
  }

  def setIsPostShuffleCache(): Lineage[T] = {
    this.isPreShuffleCache = Some(false)
    this
  }

  private[spark] def rightJoin[T](prev: Lineage[(T, Any)], next: RDD[(T, Any)]) = {
    prev.zipPartitions(next) {
      (buildIter, streamIter) =>
        val hashSet = new java.util.HashSet[T]()
        var rowKey: T = null.asInstanceOf[T]

        // Create a Hash set of buildKeys
        while (buildIter.hasNext) {
          rowKey = buildIter.next()._1
          val keyExists = hashSet.contains(rowKey)
          if (!keyExists) {
            hashSet.add(rowKey)
          }
        }

        if(hashSet.isEmpty) {
          Iterator.empty
        } else {
          streamIter.filter(current => { hashSet.contains(current._1) })
        }
    }
  }

  private[spark] def join3Way(prev: Lineage[(Long, Int)],
      next1: Lineage[(Long, Int)],
      next2: Lineage[(Long, String)]) = {
    prev.zipPartitions(next1,next2) {
      (buildIter, streamIter1, streamIter2) =>
        val hashSet = new java.util.HashSet[Int]()
        val hashMap = new java.util.HashMap[Long, CompactBuffer[Int]]()
        var rowKey: Int = 0

        while (buildIter.hasNext) {
          rowKey = buildIter.next()._2
          val keyExists = hashSet.contains(rowKey)
          if (!keyExists) {
            hashSet.add(rowKey)
          }
        }

        while(streamIter1.hasNext) {
          val current = streamIter1.next()
          if(hashSet.contains(current._2)) {
            var values = hashMap.get(current._1)
            if(values == null) {
              values = new CompactBuffer[Int]()
            }
            values += current._2
            hashMap.put(current._1, values)
          }
        }
        streamIter2.flatMap(current => {
          val values = if(hashMap.get(current._1) != null) {
            hashMap.get(current._1)
          } else {
            new CompactBuffer[Int]()
          }
          values.map(record => (record, current._2))
        })
    }
  }

  /** Returns the first parent Lineage */
  protected[spark] override def firstParent[U: ClassTag]: Lineage[U] =
    dependencies.head.rdd.asInstanceOf[Lineage[U]]

  /**
   * Return an array that contains all of the elements in this RDD.
   */
  override def collect(): Array[T] = {
    val results = lineageContext.runJob(this, (iter: Iterator[T]) => iter.toArray)

    if(lineageContext.isLineageActive) {
      lineageContext.setLastLineagePosition(this.getTap)
    }

    Array.concat(results: _*)
  }

  /**
   * Return the number of elements in the RDD.
   */
  override def count(): Long = {
    val result = lineageContext.runJob(this, Utils.getIteratorSize _).sum

    if(lineageContext.isLineageActive) {
      lineageContext.setLastLineagePosition(this.getTap)
    }

    result
  }

  /**
   * Return a new Lineage containing the distinct elements in this RDD.
   */
  override def distinct(): Lineage[T] = distinct(partitions.size)

  /**
   * Return a new Lineage containing the distinct elements in this RDD.
   */
  override def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): Lineage[T] =
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)

  /**
   * Return a new Lineage containing only the elements that satisfy a predicate.
   */
  override def filter(f: T => Boolean): Lineage[T] = new FilteredLRDD[T](this, context.clean(f))

  /**
   *  Return a new Lineage by first applying a function to all elements of this
   *  Lineage, and then flattening the results.
   */
  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): Lineage[U] =
    new FlatMappedLRDD[U, T](this, lineageContext.sparkContext.clean(f))

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  override def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): Lineage[(K, Iterable[T])] =
    groupBy[K](f, defaultPartitioner(this))

  /**
   * Return an RDD of grouped items. Each group consists of a key and a sequence of elements
   * mapping to that key. The ordering of elements within each group is not guaranteed, and
   * may even differ each time the resulting RDD is evaluated.
   *
   * Note: This operation may be very expensive. If you are grouping in order to perform an
   * aggregation (such as a sum or average) over each key, using [[PairRDDFunctions.aggregateByKey]]
   * or [[PairRDDFunctions.reduceByKey]] will provide much better performance.
   */
  override def groupBy[K](f: T => K, p: Partitioner)
      (implicit kt: ClassTag[K], ord: Ordering[K] = null)
  : Lineage[(K, Iterable[T])] = {
    val cleanF = lineageContext.sparkContext.clean(f)
    this.map(t => (cleanF(t), t)).groupByKey(p)
  }

  /**
   * Creates tuples of the elements in this RDD by applying `f`.
   */
  override def keyBy[K](f: T => K): Lineage[(K, T)] = {
    map(x => (f(x), x))
  }

  /**
   * Return a new Lineage by applying a function to all elements of this Lineage.
   */
  override def map[U: ClassTag](f: T => U): Lineage[U] = new MappedLRDD(this, sparkContext.clean(f))

  /**
   * Save this RDD as a text file, using string representations of elements.
   */
  override def saveAsTextFile(path: String) {
    // https://issues.apache.org/jira/browse/SPARK-2075
    //
    // NullWritable is a `Comparable` in Hadoop 1.+, so the compiler cannot find an implicit
    // Ordering for it and will use the default `null`. However, it's a `Comparable[NullWritable]`
    // in Hadoop 2.+, so the compiler will call the implicit `Ordering.ordered` method to create an
    // Ordering for `NullWritable`. That's why the compiler will generate different anonymous
    // classes for `saveAsTextFile` in Hadoop 1.+ and Hadoop 2.+.
    //
    // Therefore, here we provide an explicit Ordering `null` to make sure the compiler generate
    // same bytecodes for `saveAsTextFile`.
    val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    val textClassTag = implicitly[ClassTag[Text]]
    val r = this.map(x => (NullWritable.get(), new Text(x.toString)))
    lrddToPairLRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)

    if(lineageContext.isLineageActive) {
      lineageContext.setLastLineagePosition(r.getTap)
      setTap(r.getTap.get)
    }
  }

  def saveAsDBTable(url: String, username: String, password: String, path: String, driver: String): Unit = {}

  def saveAsCSVFile(path: String): Unit = {}

  /**
   * Return this RDD sorted by the given key function.
   */
  override def sortBy[K](
     f: (T) => K,
     ascending: Boolean = true,
     numPartitions: Int = this.partitions.size)
   (implicit ord: Ordering[K], ctag: ClassTag[K]): Lineage[T] =
    this.keyBy[K](f)
      .sortByKey(ascending, numPartitions)
      .values

  /**
   * Return the union of this RDD and another one. Any identical elements will appear multiple
   * times (use `.distinct()` to eliminate them).
   */
  def union(other: Lineage[T]): Lineage[T] =
    new CoalescedLRDD(new UnionLRDD(lineageContext, Array(this, other)), this.partitions.size)

  override def zipPartitions[B: ClassTag, V: ClassTag]
      (rdd2: RDD[B])
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): Lineage[V] =
    new ZippedPartitionsLRDD2[T, B, V](
      lineageContext,
      lineageContext.sparkContext.clean(f),
      this,
      rdd2.asInstanceOf[Lineage[B]],
      false
    )

  override def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
      (rdd2: RDD[B], rdd3: RDD[C])
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): Lineage[V] =
    new ZippedPartitionsLRDD3[T, B, C, V](
      lineageContext,
      lineageContext.sparkContext.clean(f),
      this,
      rdd2.asInstanceOf[Lineage[B]],
      rdd3.asInstanceOf[Lineage[C]],
      false
    )
}

object Lineage {
  implicit def castLineage5(rdd: (Any, (Long, Int))): (Int, Any) =
    (rdd._1.asInstanceOf[RecordId]._2, rdd._2)

  implicit def castLineage10(rdd: Lineage[_]): Lineage[(Int, Any)] =
    rdd.asInstanceOf[Lineage[(_, _)]].map(r => r._1 match {
      case r1: Int => (r1, r._2)
      case r2: RecordId => (r2._2, r._2)
    })

  implicit def castLineage3(rdd: Lineage[_]): TapLRDD[_] =
    rdd.asInstanceOf[TapLRDD[_]]

  implicit def castLineage4(rdd: Lineage[(RecordId, Any)]): Lineage[(RecordId, String)] =
    rdd.asInstanceOf[Lineage[(RecordId, String)]]

  implicit def castLineage12(rdd: Lineage[(Int, Any)]): Lineage[(RecordId, Any)] =
    rdd.map(r => ((0L, r._1), r._2))

  implicit def castLineage1(rdd: Lineage[_]): Lineage[(RecordId, Any)] =
    rdd.asInstanceOf[Lineage[(RecordId, Any)]]

  implicit def castShowToLineage[T](show: ShowRDD): Lineage[T] =
    show.asInstanceOf[Lineage[T]]
}
