package org.apache.spark.lineage.rdd

import org.apache.hadoop.io.{NullWritable, LongWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.Partitioner._
import org.apache.spark.SparkContext._
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
          new LineageRDD(getTap.get.asInstanceOf[Lineage[(RecordId, (Int, Int))]])//map(r => (r.asInstanceOf[((Int), (Int, Int))])))
        case tap: TapHadoopLRDD[_, _] =>
          new LineageRDD(tap.map(r => (r.asInstanceOf[(Long, Int)])).map(r => ((0L, r._2), r._1)))
        case tap: TapLRDD[_] => new LineageRDD(tap.map(r => r.asInstanceOf[(Int, Int)]).map(r => ((0L, r._1), (0L, r._2))))
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

  private[spark] def rightJoin(prev: Lineage[(RecordId, Any)], next: Lineage[(RecordId, Any)]) = {
    prev.zipPartitions(next) {
      (buildIter, streamIter) =>
        val hashSet = new java.util.HashSet[RecordId]()
        var rowKey: RecordId = null


        // Create a Hash set of buildKeys
        while (buildIter.hasNext) {
          rowKey = buildIter.next()._1
          val keyExists = hashSet.contains(rowKey)
          if (!keyExists) {
            hashSet.add(rowKey)
          }
        }

        streamIter.filter(current => { hashSet.contains(current._1) })
    }
  }

  private[spark] def rightJoinSuperShort(prev: Lineage[(Int, Any)], next: Lineage[(Int, Any)]) = {
    prev.zipPartitions(next) {
      (buildIter, streamIter) =>
        val hashSet = new java.util.HashSet[Int]()
        var rowKey: Int = null.asInstanceOf[Int]

        // Create a Hash set of buildKeys
        while (buildIter.hasNext) {
          rowKey = buildIter.next()._1
          val keyExists = hashSet.contains(rowKey)
          if (!keyExists) {
            hashSet.add(rowKey)
          }
        }

        streamIter.filter(current => { hashSet.contains(current._1) }
        )
    }
  }

  private[spark] def join3Way(prev: Lineage[(Long, Int)],
      next1: Lineage[(Long, Int)],
      next2: Lineage[(Long, String)]
    ) = {
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
  override def filter(f: T => Boolean): Lineage[T] = {
    if(!lineageContext.isLineageActive) {
      if(this.getTap.isDefined) {
        lineageContext.setCurrentLineagePosition(this.getTap)
        var result: ShowRDD = null
        this.getTap.get match {
          case _: TapPreShuffleLRDD[_] =>
            val tmp = this.getTap.get
              .getCachedData.setCaptureLineage(false)
              .asInstanceOf[Lineage[(Any, (Any, RecordId))]]
            tmp.setTap()
            result = new ShowRDD(tmp
              .map(r => ((r._1, r._2._1), r._2._2)).asInstanceOf[Lineage[(T, RecordId)]]
              .filter(r => f(r._1))
              .map(r => (r._2, r._1.toString()))
            )
            tmp.setTap(lineageContext.getCurrentLineagePosition.get)
          case _: TapPostShuffleLRDD[_] =>
            val tmp = this.getTap.get
              .getCachedData.setCaptureLineage(false)
              .asInstanceOf[Lineage[(T, RecordId)]]
            tmp.setTap()
            result = new ShowRDD(tmp.filter(r => f(r._1)).map(r => (r._2, r._1.toString())))
            tmp.setTap(lineageContext.getCurrentLineagePosition.get)
          case _ => throw new UnsupportedOperationException
        }
        result.setTap(this.getTap.get)
        result
      } else {
        this.dependencies(0).rdd match {
          case _: TapHadoopLRDD[_, _] =>
            var result: ShowRDD = null
            lineageContext.setCurrentLineagePosition(
              Some(this.dependencies(0).rdd.asInstanceOf[TapHadoopLRDD[_, _]])
            )
            result = new ShowRDD(this.dependencies(0).rdd
              .firstParent.asInstanceOf[HadoopLRDD[LongWritable, Text]]
              .map(r=> (r._1.get(), r._2.toString)).asInstanceOf[RDD[(Long, T)]]
              .filter(r => f(r._2))
              .join(this.dependencies(0).rdd.asInstanceOf[Lineage[(RecordId, (String, Long))]]
              .map(r => (r._2._2, r._1)))
              .distinct()
              .map(r => (r._2._2, r._2._1)))
            result.setTap(lineageContext.getCurrentLineagePosition.get)
            result
          case _ => new FilteredLRDD[T](this, context.clean(f))
        }
      }
    } else {
      new FilteredLRDD[T](this, context.clean(f))
    }
  }

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
  override def groupBy[K](f: T => K, p: Partitioner)(implicit kt: ClassTag[K], ord: Ordering[K] = null)
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
      lineageContext.setLastLineagePosition(this.getTap)
    }
  }

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
  implicit def castLineage1(rdd: Lineage[_]): Lineage[(RecordId, Any)] =
    rdd.asInstanceOf[Lineage[(RecordId, Any)]]

  implicit def castLineage2(rdd: Lineage[(Any, RecordId)]): Lineage[(RecordId, Any)] =
    rdd.asInstanceOf[Lineage[(RecordId, Any)]]

  implicit def castLineage3(rdd: Lineage[_]): TapLRDD[_] =
    rdd.asInstanceOf[TapLRDD[_]]

  implicit def castLineage4(rdd: Lineage[(RecordId, Any)]): Lineage[(RecordId, String)] =
    rdd.asInstanceOf[Lineage[(RecordId, String)]]

  implicit def castLineage5[T](rdd: RDD[(RecordId, T)]): Lineage[(RecordId, String)] =
    rdd.asInstanceOf[Lineage[(RecordId, String)]]

  implicit def castLineage6(rdd: Lineage[_]): Lineage[(RecordId, (String, Long))] =
    rdd.asInstanceOf[Lineage[(RecordId, (String, Long))]]

  implicit def castLineage8(rdd: Lineage[(_, _)]): Lineage[(RecordId, Any)] =
    rdd.asInstanceOf[Lineage[(RecordId, Any)]]

  implicit def castLineage9(rdd: Lineage[(_, _)]): Lineage[(Any, Any)] =
    rdd.asInstanceOf[Lineage[(Any, Any)]]

//  implicit def castLineage10(rdd: Lineage[(Any, Int)]): Lineage[(Int, Any)] =
//    rdd.asInstanceOf[Lineage[(Int, Any)]]

  implicit def castLineage11(rdd: Lineage[(RecordId, _)]): Lineage[(Int, Any)] =
    rdd.map(r => (r._1._2, r._2))

//  implicit def castLineage7(rdd: Lineage[(_, _)]): Lineage[(Int, Any)] =
//    rdd.asInstanceOf[Lineage[(Int, Any)]]

  implicit def castLineage12(rdd: Lineage[(Int, Any)]): Lineage[(RecordId, Any)] =
    rdd.map(r => ((0L, r._1), r._2))

  implicit def castShowToLineage[T](show: ShowRDD): Lineage[T] =
    show.asInstanceOf[Lineage[T]]
}
