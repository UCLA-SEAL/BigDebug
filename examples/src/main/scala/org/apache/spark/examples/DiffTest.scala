package org.apache.spark.examples

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.language.implicitConversions

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
  (@transient val prev:DiffRDDGenerator[_],
   @transient val spark:SparkContext) extends Serializable
{
  @transient var origRDD: RDD[A] = null
  @transient var incrRDD: RDD[Diff[A]] = null
  @transient var origComputationRan = false

  def this(@transient prev: DiffRDDGenerator[_]) = this(prev, prev.spark)

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

  def traverseDagFromLeaf(f: (DiffRDDGenerator[_] => Unit)): Unit =
  {
    var current: DiffRDDGenerator[_] = this
    while(current != null) {
      f(current)
      current = current.prev
    }
  }

  def traverseDagFromRoot(f: (DiffRDDGenerator[_] => Unit)): Unit =
  {
    var nodes = new ListBuffer[DiffRDDGenerator[_]]
    traverseDagFromLeaf(nodes += _)
    nodes = nodes.reverse
    nodes.foreach(f)
  }

  def assembleThisOrigComputation(): Unit
  def assembleThisIncrComputation(): Unit

  def assembleOrigComputation(): Unit =
  {
    if(prev != null) {
      prev.assembleOrigComputation()
    }
    this.assembleThisOrigComputation()
  }

  def assembleIncrComputation(): Unit =
  {
    assert(this.origComputationRan)
    if(prev != null) {
      prev.assembleIncrComputation()
    }
    this.assembleThisIncrComputation()
  }

  def setOrigComputationRan(): Unit = traverseDagFromRoot(_.origComputationRan = true)

  def generateDiff[B: ClassTag](rdd1: RDD[B], rdd2: RDD[B]): RDD[Diff[B]] =
  {
    val additions = multiSetDifference(rdd1, rdd2)
    val removals = multiSetDifference(rdd2, rdd1)
    (additions.map(Added(_)): RDD[Diff[B]]) ++ removals.map(Removed(_))
  }

  def textFile(path: String, diffPath: String): DiffRDDGenerator[String] =
  {
    assert(origRDD == null && incrRDD == null)
    new DiffTextFileGenerator(path, diffPath, spark)
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
  def reduceByKey(f: (V, V) => V, finv: (V, V) => V): DiffRDDGenerator[(K, V)] = {
    new DiffReduceByKeyGenerator(f, finv, self)
  }

  def reduceByKey(f: (V, V) => V, finv: (V, V) => V, fzero: V => Boolean): DiffRDDGenerator[(K, V)] = {
    new DiffReduceByKeyGenerator(f, finv, fzero, self)
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
    val changes: RDD[Diff[String]] = spark.textFile(diffPath).map(line =>
      if(line(0) == '+') {
        Added(line.substring(1))
      } else {
        Removed(line.substring(1))
      }
    )
    val inputAdditions:RDD[Diff[String]] = changes.filter({
      case Added(_) => true
      case Removed(_) => false
    })

    val removals = changes.filter({
      case Added(_) => false
      case Removed(_) => true
    })
    //check to make sure the removals actually belong to the input
    val inputRemovals:RDD[Diff[String]] = removals.map(_.element).intersection(origRDD).map(Removed(_))
    val deltaInput = inputRemovals ++ inputAdditions

    incrRDD = deltaInput
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
  var filterChanged: Boolean = false

  def incrementallySetNewFilter(f1: A => Boolean): Unit =
  {
    assert(origComputationRan)
    f = f1
    filterChanged = true
  }

  override def assembleIncrComputation(): Unit =
  {
    if(filterChanged) {
      val newFilter = prev.origRDD.filter(f)
      incrRDD = generateDiff(newFilter, origRDD)
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
    val changesRDD = prev.incrRDD.map({
      case Added((k, v)) => (k, Added(v))
      case Removed((k, v)) => (k, Removed(v))
    })

    def computeChanges[VV](origValues: Iterable[VV], changedValues: Iterable[Diff[VV]], f: ((VV, VV) => VV), finv: ((VV, VV) => VV)): Iterable[VV] = {
      assert(!(origValues.isEmpty && changedValues.isEmpty))
      val startingValue = if (origValues.isEmpty) changedValues.head.element else origValues.head
      val values = if (origValues.isEmpty) changedValues.tail else changedValues

      List(values.foldLeft(startingValue)((acc, change) => change match {
        case Added(x) => f(acc, x)
        case Removed(x) => finv(acc, x)
      }))
    }

    val newReducedRDD = origRDD.cogroup(changesRDD).flatMapValues(pair => computeChanges(pair._1, pair._2, f, finv))

    def includeRelevantChanges(key: K, newValue: Iterable[V], oldValue: Iterable[V]): Iterable[Diff[(K, V)]] = {
      var changes: List[Diff[(K, V)]] = List()
      if (oldValue.isEmpty) {
        changes = Added((key, newValue.head)) :: changes
      }
      else if (oldValue.head != newValue.head) {
        changes = Removed((key, oldValue.head)) :: changes
        //if the change didn't make it zero
        if(!fzero(newValue.head)) {
          changes = Added((key, newValue.head)) :: changes
        }
      }
      changes
    }

    val incrementalReduceRDD: RDD[Diff[(K, V)]] = newReducedRDD.cogroup(origRDD).flatMap(
    { case (key: K, (newValue: Iterable[V], oldValue: Iterable[V])) =>
      includeRelevantChanges(key, newValue, oldValue)
    })

    incrRDD = incrementalReduceRDD
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

  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("DiffTest")
    val spark = new SparkContext(conf)

    val input = new DiffTextFileGenerator("README.md", "README.md.diff", spark).flatMap(line => line.trim().split(' '))
    //val workflow = input.flatMap(line => line.trim().split(' ')).filter({w: String => w.length > 0}).map((_, 1)).reduceByKey(_ + _, _ - _)
    val filter = input.filter({_ => true})
    val workflow = filter.map((_, 1)).reduceByKey(_ + _, _ - _, _ == 0)
    val origResults = workflow.collect()

    filter.incrementallySetNewFilter(_.length() > 4)
    val incrResults = workflow.incrCollect()
    spark.stop()

    //origResults.foreach(println)
    incrResults.foreach(println)
  }
}