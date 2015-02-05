package org.apache.spark.examples

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.implicitConversions
import scala.reflect.ClassTag

abstract class Diff[+A] extends Serializable
{
  val element: A

  def map[V](f: A => V): Diff[V] =
  {
    this match {
      case Added(e:A) => Added(f(e))
      case Removed(e:A) => Removed(f(e))
    }
  }

  def flatMap[B](f: A => Iterable[B]): Iterable[Diff[B]] =
  {
    this match {
      case Added(e:A) => f(e).map(Added(_))
      case Removed(e: A) => f(e).map(Removed(_))
    }
  }

  def predicateHolds(f: A => Boolean): Boolean =
  {
    this match {
      case Added(e) => f(e)
      case Removed(e) => f(e)
    }
  }

  override def toString: String =
  {
    this match {
      case Added(e:A) => "Added(" + e + ")"
      case Removed(e:A) => "Removed(" + e + ")"
    }
  }
}
case class Added[A](element: A) extends Diff[A]
case class Removed[A](element: A) extends Diff[A]

abstract class DiffRDDGenerator[A: ClassTag]
  (@transient var prev:Seq[DiffRDDGenerator[_]],
   @transient val spark:SparkContext) extends Serializable
{
  @transient var origRDD: RDD[A] = null
  @transient var incrRDD: RDD[Diff[A]] = null
  @transient var origComputationRan = false

  def this(@transient onePrev: DiffRDDGenerator[_]) = this(List(onePrev), onePrev.spark)

  implicit def diffGeneratorToPairRDDFunctions[K, V]
  (diff: DiffRDDGenerator[(K, V)])
  (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairDiffGeneratorFunctions[K, V] =
  {
    new PairDiffGeneratorFunctions(diff)
  }

  def setDifference[B: ClassTag](rdd1: RDD[B], rdd2: RDD[B]): RDD[B] =
  {
    rdd1.map((_, null)).cogroup(rdd2.map((_, null))).filter({
      case (_, (left, right)) => left.nonEmpty && right.isEmpty}).keys
  }

  def multiSetDifference[B: ClassTag](rdd1: RDD[B], rdd2: RDD[B]): RDD[B] =
  {
    def makeMultiset(rdd: RDD[B]): RDD[(B, Int)] =
    {
      rdd.map((_, 1)).reduceByKey(_ + _)
    }

    def flattenMultiSet(rdd: RDD[(B, Int)]): RDD[B] =
    {
      rdd.flatMap({case (value, count) =>
        List.fill(count)(value)
      })
    }

    val multiset1 = makeMultiset(rdd1)
    val multiset2 = makeMultiset(rdd2)

    def countsDifference(value: B, count1List: Iterable[Int], count2List: Iterable[Int]): List[(B, Int)] =
    {
      (count1List.isEmpty, count2List.isEmpty) match {
        case (true, _) => List()
        case (false, true) => List((value, count1List.head))
        case (false, false) =>
          if(count1List.head > count2List.head) {
            List((value, count1List.head - count2List.head))
          }
          else {
            List()
          }
      }
    }

    flattenMultiSet(multiset1.cogroup(multiset2).flatMap({case (value, (count1List, count2List)) =>
      countsDifference(value, count1List, count2List)
    }))
  }

  def iterateUpstream(f: (DiffRDDGenerator[_] => Unit)): Unit =
  {
    f(this)
    if(prev != null)
      for (p <- prev) p.iterateUpstream(f)
  }

  def assembleThisOrigComputation(): Unit
  def assembleThisIncrComputation(): Unit

  def assembleOrigComputation(): Unit =
  {
    if(prev != null) {
      for (p <- prev) p.assembleOrigComputation()
    }
    this.assembleThisOrigComputation()
  }

  def assembleIncrComputation(): Unit =
  {
    assert(this.origComputationRan)
    if(prev != null) {
      for (p <- prev) p.assembleIncrComputation()
    }
    this.assembleThisIncrComputation()
  }

  def setOrigComputationRan(): Unit = iterateUpstream(_.origComputationRan = true)

  def generateDiff[B: ClassTag](rdd1: RDD[B], rdd2: RDD[B]): RDD[Diff[B]] =
  {
    val additions = multiSetDifference(rdd1, rdd2)
    val removals = multiSetDifference(rdd2, rdd1)
    (additions.map(Added(_)): RDD[Diff[B]]) ++ removals.map(Removed(_))
  }

  def flatMap[B: ClassTag](f: A => Iterable[B]): DiffFlatMapGenerator[A, B] =
  {
    new DiffFlatMapGenerator(f, this)
  }

  def map[B: ClassTag](f: A => B): DiffMapGenerator[A, B] =
  {
    new DiffMapGenerator(f, this)
  }

  def filter(f: A => Boolean): DiffFilterGenerator[A] =
  {
    new DiffFilterGenerator(f, this)
  }

  def collect() =
  {
    assembleOrigComputation()
    val res = origRDD.collect()
    setOrigComputationRan()
    res
  }

  def incrCollect() =
  {
    assembleIncrComputation()
    incrRDD.collect()
  }
}

class PairDiffGeneratorFunctions[K, V](self: DiffRDDGenerator[(K, V)])
                            (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
{
  def liftKeys[KK, VV](rdd: RDD[Diff[(KK, VV)]]): RDD[(KK, Diff[VV])] =
  {
    rdd.map({
      case Added((k, v)) => (k, Added(v))
      case Removed((k, v)) => (k, Removed(v))
    })
  }

  def flattenKeys[KK, VV](rdd: RDD[(KK, Diff[VV])]): RDD[Diff[(KK, VV)]] =
  {
    rdd.map({
      case (key, Added(x)) => Added(key, x)
      case (key, Removed(x)) => Removed(key, x)
    })
  }

  def reduceByKey(f: (V, V) => V, finv: (V, V) => V): DiffRDDGenerator[(K, V)] =
  {
    new DiffReduceByKeyGenerator(f, finv, self)
  }

  def reduceByKey(f: (V, V) => V, finv: (V, V) => V, fzero: V => Boolean): DiffRDDGenerator[(K, V)] =
  {
    new DiffReduceByKeyGenerator(f, finv, fzero, self)
  }

  def join[W: ClassTag](other: DiffRDDGenerator[(K, W)]) =
  {
    new DiffJoinGenerator(self, other)
  }
}

class DiffSeqGenerator[A: ClassTag](data: Seq[A], diffData: Seq[Diff[A]], spark: SparkContext)
  extends DiffRDDGenerator[A](null, spark)
{
  override def assembleThisOrigComputation(): Unit =
  {
    origRDD = spark.makeRDD(data)
  }

  override def assembleThisIncrComputation(): Unit =
  {
    incrRDD = spark.makeRDD(diffData)
  }
}

class DiffTextFileGenerator(path: String, diffPath: String, spark: SparkContext)
  extends DiffRDDGenerator[String](null, spark)
{
  override def assembleThisOrigComputation(): Unit =
  {
    origRDD = spark.textFile(path).cache()
  }

  override def assembleThisIncrComputation(): Unit =
  {
    //we're assuming that a line marked to be removed here actually exists in the original file
    incrRDD = spark.textFile(diffPath).map(line =>
      if(line(0) == '+') {
        Added(line.substring(1))
      } else {
        Removed(line.substring(1))
      }
    )
  }
}

class DiffFlatMapGenerator[A: ClassTag, B: ClassTag](f: A => Iterable[B], prev: DiffRDDGenerator[A])
  extends DiffRDDGenerator[B](prev)
{
  override def assembleThisOrigComputation(): Unit =
  {
    origRDD = prev.origRDD.flatMap(f)
  }

  override def assembleThisIncrComputation(): Unit =
  {
    incrRDD = prev.incrRDD.flatMap(_.flatMap(f))
  }
}

class DiffMapGenerator[A: ClassTag, B: ClassTag](f: A => B, prev: DiffRDDGenerator[A])
  extends DiffRDDGenerator[B](prev)
{
  override def assembleThisOrigComputation(): Unit =
  {
    origRDD = prev.origRDD.map(f)
  }

  override def assembleThisIncrComputation(): Unit =
  {
    incrRDD = prev.incrRDD.map(_.map(f))
  }
}

class DiffFilterGenerator[A: ClassTag]
  (var f: A => Boolean,
   prev: DiffRDDGenerator[A])
  extends DiffRDDGenerator[A](prev)
{
  var entireFilterChanged: Boolean = false
  
  var incrementalFilterChanged: Boolean = false
  var incrementalAdditionsFunction: (A => Boolean) = null
  var incrementalRemovalsFunction: (A => Boolean) = null

  def setNewFilter(f1: A => Boolean): Unit =
  {
    assert(origComputationRan)
    f = f1
    entireFilterChanged = true
  }

  def incrementallySetNewFilter(additions: A => Boolean = null, removals: A => Boolean = null) =
  {
    assert(origComputationRan)
    assert(additions != null || removals != null)
    incrementalFilterChanged = true
    incrementalAdditionsFunction = if(additions == null) {_ => false} else additions
    incrementalRemovalsFunction = if(removals == null) {_ => false} else removals
  }

  override def assembleIncrComputation(): Unit =
  {
    if(entireFilterChanged) {
      val newFilter = prev.origRDD.filter(f)
      incrRDD = generateDiff(newFilter, origRDD)
    }
    else if(incrementalFilterChanged) {
      val additions: RDD[Diff[A]] = prev.origRDD.filter(incrementalAdditionsFunction).map(Added(_))
      val removals: RDD[Diff[A]] = prev.origRDD.filter(incrementalRemovalsFunction).map(Removed(_))
      incrRDD = additions ++ removals
    }
    else {
      super.assembleIncrComputation()
    }
  }

  override def assembleThisOrigComputation(): Unit =
  {
    origRDD = prev.origRDD.filter(f)
  }

  override def assembleThisIncrComputation(): Unit =
  {
    incrRDD = prev.incrRDD.filter(_.predicateHolds(f))
  }
}

class DiffReduceByKeyGenerator[K: ClassTag, V: ClassTag]
  (f: (V, V) => V,
   finv: (V, V) => V,
   fzero: V => Boolean,
   prev:DiffRDDGenerator[(K, V)])
  extends DiffRDDGenerator[(K, V)](prev)
{
  def this(f:(V, V) => V, finv: (V, V) => V, prev:DiffRDDGenerator[(K, V)]) = this(f, finv, _ => false, prev)

  override def assembleThisOrigComputation(): Unit =
  {
    origRDD = prev.origRDD.reduceByKey(f)
  }

  override def assembleThisIncrComputation(): Unit =
  {
    //get a common key between orig and changes
    val changes = this.liftKeys(prev.incrRDD)
    val additions = changes.flatMap({
      case (key, Added(x)) => List((key, x))
      case _ => List()
    })
    val removals = changes.flatMap({
      case (key, Removed(x)) => List((key, x))
      case _ => List()
    })

    val totalAdditions = additions.reduceByKey(f)
    val totalRemovals = removals.reduceByKey(f)

    def computeChanges(key: K, origValueList: Iterable[V], additions: Iterable[V], removals: Iterable[V]): Iterable[Diff[(K, V)]] =
    {
      val newValue: V =
        (origValueList.nonEmpty, additions.nonEmpty, removals.nonEmpty) match {
          case (true, true, true) => finv(f(origValueList.head, additions.head), removals.head)
          case (true, true, false) => f(origValueList.head, additions.head)
          case (true, false, true) => finv(origValueList.head, removals.head)
          case (true, false, false) => origValueList.head
          case (false, true, true) => finv(additions.head, removals.head)
          case (false, true, false) => additions.head
          case _ => assert(assertion = false)
            origValueList.head //to satisfy the type system
        }

      var changes: List[Diff[(K, V)]] = List()
      if (origValueList.isEmpty) {
        changes = Added((key, newValue)) :: changes
      }
      else if (origValueList.head != newValue) {
        changes = Removed((key, origValueList.head)) :: changes
        //if the change didn't make it zero
        if(!fzero(newValue)) {
          changes = Added((key, newValue)) :: changes
        }
      }
      changes
    }

    val reducedRDD = origRDD.cogroup(totalAdditions, totalRemovals).flatMap({
      case (key: K, (origValueList: Iterable[V], additions: Iterable[V], removals: Iterable[V])) =>
        computeChanges(key, origValueList, additions, removals)
    })

    incrRDD = reducedRDD
  }
}

class DiffJoinGenerator[K: ClassTag, V: ClassTag, W: ClassTag]
  (left: DiffRDDGenerator[(K, V)], 
   right: DiffRDDGenerator[(K, W)])
  extends DiffRDDGenerator[(K, (V, W))](List(left, right), left.spark)
{
  override def assembleThisOrigComputation(): Unit =
  {
    origRDD = left.origRDD.join(right.origRDD)
  }

  override def assembleThisIncrComputation(): Unit =
  {
    val kLeft = this.liftKeys(left.incrRDD)
    val kRight = this.liftKeys(right.incrRDD)

    val bothChanges: RDD[(K, Diff[(V, W)])] = kLeft.join(kRight).mapValues({
      case (Added(x), Added(y)) => Added((x, y))
      case (Added(x), Removed(y)) => Removed((x, y))
      case (Removed(x), Added(y)) => Removed((x, y))
      case (Removed(x), Removed(y)) => Added((x, y)) //we double count below, to compensate for that
    })

    val leftChanges: RDD[(K, Diff[(V, W)])] = kLeft.join(right.origRDD).mapValues({
      case (Added(val1), val2) => Added(val1, val2)
      case (Removed(val1), val2) => Removed(val1, val2)
    })

    val rightChanges: RDD[(K, Diff[(V, W)])] = left.origRDD.join(kRight).mapValues({
      case (val1, Added(val2)) => Added(val1, val2)
      case (val1, Removed(val2)) => Removed(val1, val2)
    })

    //the order here matters coz we want to add the to-be-doubly removed values first before removing them twice
    val kIncrRDD = bothChanges ++ leftChanges ++ rightChanges
    incrRDD = this.flattenKeys(kIncrRDD)
  }
}

object DiffTest
{
  implicit def diffGeneratorToPairRDDFunctions[K, V]
    (diff: DiffRDDGenerator[(K, V)])
    (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): PairDiffGeneratorFunctions[K, V] =
  {
    new PairDiffGeneratorFunctions(diff)
  }

  def deltaDataTest(spark:SparkContext) =
  {
    val workflow = new DiffTextFileGenerator("README.md", "README.md.diff", spark).flatMap(line => line.trim().split(' ')).map((_, 1)).reduceByKey(_ + _, _ - _, _ == 0)
    val origResults = workflow.collect()
    val incrResults = workflow.incrCollect()

    origResults.foreach(println)
    incrResults.foreach(println)
  }

  def changeFilterTest(spark:SparkContext) =
  {
    val input = new DiffTextFileGenerator("README.md", null, spark).flatMap(line => line.trim().split(' '))
    val filter = input.filter({_ => true})
    val workflow = filter.map((_, 1)).reduceByKey(_ + _, _ - _, _ == 0)
    val origResults = workflow.collect()

    filter.setNewFilter(_ != "can")
    val incrResults = workflow.incrCollect()

    //origResults.foreach(println)
    incrResults.foreach(println)
  }

  def incrementalFilterTest(spark:SparkContext) =
  {
    val input = new DiffTextFileGenerator("README.md", null, spark).flatMap(line => line.trim().split(' '))
    val filter = input.filter(_.length() >= 3)
    val workflow = filter.map((_, 1)).reduceByKey(_ + _, _ - _, _ == 0)
    val origResults = workflow.collect()

    filter.incrementallySetNewFilter(removals = _.length() == 3)
    val incrResults = workflow.incrCollect()

    //origResults.foreach(println)
    incrResults.foreach(println)
  }

  def joinTest(spark:SparkContext) =
  {
    val inputLeft = List((1, 'a'), (2, 'b'), (3, 'c'), (100, 'z'))
    val inputRight = List((1, 'A'), (2, 'B'), (4, 'D'), (1, 'X'), (100, 'Z'))
    val dInputLeft = List(Added(2, 'e'), Removed(100, 'z'), Added(101, 'm'))
    val dInputRight = List(Removed(1, 'A'), Removed(1, 'X'), Removed(100, 'Z'), Added(101, 'M'))

    val left = new DiffSeqGenerator(inputLeft, dInputLeft, spark)
    val right = new DiffSeqGenerator(inputRight, dInputRight, spark)
    val workflow = left.join(right)

    val origResults = workflow.collect()
    val incrResults = workflow.incrCollect()

    println("orig results")
    origResults.foreach(println)
    println("incr results")
    incrResults.foreach(println)
  }

  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("DiffTest")
    val spark = new SparkContext(conf)

    incrementalFilterTest(spark)
    readLine()
    spark.stop()
  }
}