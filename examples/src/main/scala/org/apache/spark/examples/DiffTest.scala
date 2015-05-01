package org.apache.spark.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.{ClassTag, _}
import scala.util.Random

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

class Multiset[A: ClassTag](@transient val multiset: RDD[(A, Int)]) extends Serializable
{
  def flattenMultiset(): RDD[A] =
  {
    multiset.flatMap({case (value, count) =>
      List.fill(count)(value)
    })
  }

  def difference(multiset2: Multiset[A]): Multiset[A] =
  {
    def countsDifference(value: A, count1List: Iterable[Int], count2List: Iterable[Int]): List[(A, Int)] =
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

    new Multiset(multiset.cogroup(multiset2.multiset).flatMap({case (value, (count1List, count2List)) =>
      countsDifference(value, count1List, count2List)
    }))
  }

  def union(multiset2: Multiset[A]): Multiset[A] =
  {
    def countsSum(count1List: Iterable[Int], count2List: Iterable[Int]): Int =
    {
      (count1List.nonEmpty, count2List.nonEmpty) match {
        case (true, true) => count1List.head + count2List.head
        case (true, false) => count1List.head
        case (false, true) => count2List.head
        case (false, false) => assert(assertion = false, message = "Cannot have both keys with no values")
          0
      }
    }

    new Multiset(multiset.cogroup(multiset2.multiset).mapValues(pair => countsSum(pair._1, pair._2)))
  }

  //the boolean flag indicates whether the element was introduced into the multiset due to the diff
  //this flag is used for distinct operator
  def applyDiff(diff: RDD[Diff[A]]): RDD[(A, (Int, Boolean))] =
  {
    val additions = diff.filter({
      case Added(_) => true
      case Removed(_) => false
    }).map(_.element)

    val additionsSet = Multiset.makeMultiset(additions).multiset

    val removals = diff.filter({
      case Added(_) => false
      case Removed(_) => true
    }).map(_.element)

    val removalsSet = Multiset.makeMultiset(removals).multiset.mapValues(-1 * _)
    val diffMultiset = additionsSet ++ removalsSet

    def countsSum(count1List: Iterable[Int], count2List: Iterable[Int]): (Int, Boolean) =
    {
      (count1List.nonEmpty, count2List.nonEmpty) match {
        case (true, true) => (count1List.head + count2List.head, false)
        case (true, false) => (count1List.head, false)
        case (false, true) => (count2List.head, true)
        case (false, false) => assert(assertion = false, message = "Cannot have both keys with no values")
          (0, false)
      }
    }

    multiset.cogroup(diffMultiset).mapValues(pair => countsSum(pair._1, pair._2))
  }
}

object Multiset
{
  def makeMultiset[A: ClassTag](@transient baseRDD: RDD[A]): Multiset[A] = new Multiset(baseRDD.map((_, 1)).reduceByKey(_ + _))
}

abstract class DiffRDDGenerator[A: ClassTag]
  (@transient var parents:Seq[DiffRDDGenerator[_]],
   @transient val spark:SparkContext) extends Serializable
{
  @transient var origRDD: RDD[A] = null
  @transient var incrRDD: RDD[Diff[A]] = null
  @transient var origComputationRan = false
  @transient private var shouldCacheOrigOutput = false

  def this(@transient onePrev: DiffRDDGenerator[_]) = this(List(onePrev), onePrev.spark)

  def setOrigCached(): Unit = shouldCacheOrigOutput = true

  def setDifference[B: ClassTag](rdd1: RDD[B], rdd2: RDD[B]): RDD[B] =
  {
    rdd1.map((_, null)).cogroup(rdd2.map((_, null))).filter({
      case (_, (left, right)) => left.nonEmpty && right.isEmpty}).keys
  }

  def multiSetDifference[B: ClassTag](rdd1: RDD[B], rdd2: RDD[B]): RDD[B] =
  {
    val multiset1 = Multiset.makeMultiset(rdd1)
    val multiset2 = Multiset.makeMultiset(rdd2)

    multiset1.difference(multiset2).flattenMultiset()
  }

  def additions[AA: ClassTag](diff: RDD[Diff[AA]]): RDD[AA] = diff.flatMap({
    case Added(x: AA) => List(x)
    case _ => List()
  })

  def removals[AA: ClassTag](diff: RDD[Diff[AA]]): RDD[AA] = diff.flatMap({
    case Removed(x: AA) => List(x)
    case _ => List()
  })

  def additionsInPairs[K: ClassTag, V: ClassTag](diff: RDD[(K, Diff[V])]): RDD[(K, V)] = diff.flatMap({
    case (key, Added(x)) => List((key, x))
    case _ => List()
  })

  def removalsInPairs[K: ClassTag, V: ClassTag](diff: RDD[(K, Diff[V])]): RDD[(K, V)] = diff.flatMap({
    case (key, Removed(x)) => List((key, x))
    case _ => List()
  })

  def iterateUpstream(f: (DiffRDDGenerator[_] => Unit)): Unit =
  {
    f(this)
    if(parents != null)
      for (p <- parents) p.iterateUpstream(f)
  }

  def assembleThisOrigComputation(): Unit
  def assembleThisIncrComputation(): Unit

  def assembleOrigComputation(): Unit =
  {
    if(parents != null) {
      for (p <- parents) p.assembleOrigComputation()
    }
    this.assembleThisOrigComputation()
    if(this.shouldCacheOrigOutput) {
      this.origRDD.cache()
    }
  }

  def assembleIncrComputation(): Unit =
  {
    assert(this.origComputationRan)
    if(parents != null) {
      for (p <- parents) p.assembleIncrComputation()
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

  def liftKeys[K, V](rdd: RDD[Diff[(K, V)]]): RDD[(K, Diff[V])] =
  {
    rdd.map({
      case Added((k, v)) => (k, Added(v))
      case Removed((k, v)) => (k, Removed(v))
    })
  }

  def flattenKeys[K, V](rdd: RDD[(K, Diff[V])]): RDD[Diff[(K, V)]] =
  {
    rdd.map({
      case (key, Added(x)) => Added(key, x)
      case (key, Removed(x)) => Removed(key, x)
    })
  }

  def flatMap[B: ClassTag](f: A => Iterable[B]): DiffFlatMapGenerator[A, B] =
  {
    new DiffFlatMapGenerator(f, this)
  }

  //def map[B: ClassTag](f: A => B): DiffMapGenerator[A, B] =
  def map[B: ClassTag](f: A => B): DiffRDDGenerator[B] =
  {
    new DiffMapGenerator(f, this)
  }

  def filter(f: A => Boolean): DiffFilterGenerator[A] =
  {
    new DiffFilterGenerator(f, this)
  }

  def union(other: DiffRDDGenerator[A]): DiffUnionGenerator[A] =
  {
    new DiffUnionGenerator(this, other)
  }

  def ++(other: DiffRDDGenerator[A]): DiffRDDGenerator[A] = this.union(other)

  def distinct() =
  {
    new DiffDistinctGenerator(this)
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
    this.assembleIncrComputation()
    incrRDD.collect()
  }

  def incrOutput() =
  {
    this.assembleIncrComputation()
    new Multiset(Multiset.makeMultiset(this.origRDD).applyDiff(this.incrRDD).mapValues(_._1)).flattenMultiset().collect()
  }

  def fixPoint[B: ClassTag](dagForIteration: (DiffRDDGenerator[A], Int) => DiffRDDGenerator[A],
                            resultForIteration: (DiffRDDGenerator[A], Int, Boolean) => B): DiffFixPoint[A, B] =
  {
    new DiffFixPoint(this, dagForIteration, resultForIteration)
  }

  def count(): Long =
  {
    assembleOrigComputation()
    setOrigComputationRan()
    this.origRDD.cache().count()
  }

  def incrCount(): Long =
  {
    assembleIncrComputation()

    val additions = this.incrRDD.filter({
      case Added(_) => true
      case _ => false
    })

    val removals = this.incrRDD.filter({
      case Removed(_) => true
      case _ => false
    })

    val orig = this.origRDD.count()
    val rem = removals.count()
    val add = additions.count()
    //println(s"Orig count: $orig, removals: $rem, additions: $add")
    orig - rem + add
  }
}

class PairDiffGeneratorFunctions[K, V](@transient self: DiffRDDGenerator[(K, V)])
                            (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null)
{
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
    origRDD = spark.textFile(path)
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
  var filterChanged: Boolean = false
  var incrementalAdditionsFunction: (A => Boolean) = null
  var incrementalRemovalsFunction: (A => Boolean) = null
  prev.setOrigCached() //for the incremental filter

  def setNewFilter(f1: A => Boolean): Unit =
  {
    assert(origComputationRan)
    def additions(x: A) = f1 (x) && !f(x)
    def removals(x: A) = f(x) && !f1(x)
    filterChanged = true
    incrementalAdditionsFunction = additions
    incrementalRemovalsFunction = removals
  }

  override def assembleIncrComputation(): Unit =
  {
    if(filterChanged) {
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
  this.setOrigCached()

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
      case (x, (a: Iterable[V], b: Iterable[V], c: Iterable[V])) => throw new Exception("Unexpected case " + x.getClass + " expected: " + classTag[K])
    })

    incrRDD = reducedRDD
  }
}

class DiffUnionGenerator[A: ClassTag] (left: DiffRDDGenerator[A], right: DiffRDDGenerator[A])
  extends DiffRDDGenerator[A](List(left, right), left.spark)
{
  override def assembleThisOrigComputation(): Unit =
  {
    origRDD = left.origRDD ++ right.origRDD
  }

  override def assembleThisIncrComputation(): Unit =
  {
    incrRDD = left.incrRDD ++ right.incrRDD
  }
}

class DiffDistinctGenerator[A: ClassTag](prev: DiffRDDGenerator[A])
  extends DiffRDDGenerator[A](prev)
{
  var multiset: Multiset[A] = null

  override def assembleThisOrigComputation(): Unit =
  {
    multiset = Multiset.makeMultiset(prev.origRDD)
    this.origRDD = multiset.multiset.cache().map(_._1)
  }

  override def assembleThisIncrComputation(): Unit =
  {
    this.incrRDD = multiset.applyDiff(prev.incrRDD).flatMap({
      case (k, (0, _)) => List(Removed(k))
      case (k, (count, true)) => List(Added(k))
      case(_, (_, false)) => List()
    })
  }
}

class DiffJoinGenerator[K: ClassTag, V: ClassTag, W: ClassTag]
  (left: DiffRDDGenerator[(K, V)], 
   right: DiffRDDGenerator[(K, W)])
  extends DiffRDDGenerator[(K, (V, W))](List(left, right), left.spark)
{
  left.setOrigCached()
  right.setOrigCached()

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

class DiffFixPoint[A: ClassTag, B: ClassTag](initialDag: DiffRDDGenerator[A],
                                             dagForIteration: (DiffRDDGenerator[A], Int) => DiffRDDGenerator[A],
                                             resultForIteration: (DiffRDDGenerator[A], Int, Boolean) => B)
{
  val dagsForOrigIteration: scala.collection.mutable.ListBuffer[(DiffRDDGenerator[A])] = new scala.collection.mutable.ListBuffer()

  def fixPoint(): DiffRDDGenerator[A] =
  {
    var dag = initialDag
    var iter = 0
    var result = resultForIteration(dag, iter, false)
    dagsForOrigIteration += dag

    var prev_result = result
    var prev_dag = dag

    do {
      iter = iter + 1
      prev_result = result
      prev_dag = dag
      dag = dagForIteration(prev_dag, iter)
      result = resultForIteration(dag, iter, false)
      if (dag.origRDD != null) { dag.origRDD.cache() }
      dagsForOrigIteration += dag
    } while(prev_result != result)

    dag
  }

  def incrFixPoint(): DiffRDDGenerator[A] =
  {
    var dag = dagsForOrigIteration.head
    var origDags = dagsForOrigIteration.tail
    var iter = 0
    var result = resultForIteration(dag, iter, true)
    var prev_result = result
    do {
      iter = iter + 1
      prev_result = result
      dag = origDags.head
      result = resultForIteration(dag, iter, true)
      if (dag.incrRDD != null) { dag.incrRDD.cache() }
      origDags = origDags.tail
    } while (prev_result != result)

    dag
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

  def time(fn: Unit => Unit, name: String): Unit =
  {
    val start = System.currentTimeMillis()
    fn()
    println(s"$name took ${System.currentTimeMillis() - start}ms")
  }

  def deltaDataTest(spark:SparkContext) =
  {
    val workflow = new DiffTextFileGenerator("README.md", "README.md.diff", spark).flatMap(line => line.trim().split(' ')).map((_, 1)).reduceByKey(_ + _, _ - _, _ == 0)
    val origResults = workflow.collect()
    val incrResults = workflow.incrCollect()

    println("Orig results")
    origResults.foreach(println)
    println("Incr results")
    incrResults.foreach(println)
  }

  def changeFilterTest(spark:SparkContext) =
  {
    //val input = new DiffTextFileGenerator("hdfs://scai01.cs.ucla.edu:9000/clash/output20.txt", null, spark).flatMap(line => line.trim().split(' '))

    val input = new DiffTextFileGenerator("README.md", null, spark).flatMap(line => line.trim().split(' '))
    val filter = input.filter({_ => true})
    val workflow = filter.map((_, 1)).reduceByKey(_ + _, _ - _, _ == 0)
    val origResults = workflow.collect()

    filter.setNewFilter(_ != "can")
    val incrResults = workflow.incrCollect()

    //origResults.foreach(println)
    incrResults.foreach(println)
  }

  def changeFilterTest2(spark:SparkContext) =
  {
    val input = new DiffTextFileGenerator("README.md", null, spark).flatMap(line => line.trim().split(' '))
    val filter = input.filter(_.length() >= 3)
    val workflow = filter.map((_, 1)).reduceByKey(_ + _, _ - _, _ == 0)
    time(_ => workflow.collect(), "orig computation done")

    filter.setNewFilter(_.length() > 3)
    var incrResults: Array[Diff[(String, Int)]] = null
    time(_ => incrResults = workflow.incrCollect(), "incr computation done")

    //origResults.foreach(println)
    incrResults.foreach(println)
  }

  def changeFilterTest3(spark:SparkContext) =
  {
    val input = new DiffTextFileGenerator("README.md", null, spark).flatMap(line => line.trim().split(' ')).map((_, 1)).reduceByKey(_ + _, _ - _, _ == 0)
    val filter = input.filter(_ => true)
    val workflow = filter
    time(_ => workflow.collect(), "orig computation done")

    filter.setNewFilter(r => !r._1.contains("ar"))
    var incrResults: Array[Diff[(String, Int)]] = null
    time(_ => incrResults = workflow.incrCollect(), "incr computation done")

    //origResults.foreach(println)
    incrResults.foreach(println)
  }

  def distinctTest(spark: SparkContext) =
  {
    val workflow = new DiffSeqGenerator(List("a","b","c","b","a"), List(Added("d"), Removed("b"), Removed("b"), Added("c")), spark).distinct()
    time(_ => workflow.collect(), "orig distinct")
    time(_ => workflow.incrCollect(), "incr distinct")
    workflow.incrCollect().foreach(println)
  }

  def distinctTest2(spark: SparkContext) =
  {
    val workflow = new DiffTextFileGenerator("README.md", "README.md.diff", spark).flatMap(_.trim().split(' ')).distinct()
    time(_ => workflow.collect(), "orig distinct")
    time(_ => workflow.incrCollect(), "incr distinct")
    workflow.incrCollect().foreach(println)
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

  def TCTest(spark: SparkContext) =
  {
    val tc: DiffSeqGenerator[(Int, Int)] = new DiffSeqGenerator(
      List((1,2), (2, 3), (3, 4), (4, 5), (5, 6), (100, 101)),
      List(Removed((2, 3))),
      spark)
    val edges: DiffRDDGenerator[(Int, Int)] = tc.map(x => (x._2, x._1))

    def resultForIteration(dag: DiffRDDGenerator[(Int, Int)], iter: Int, incr: Boolean) =
    {
      if(incr) {
        val res = dag.incrOutput().toSet
//        println(s"Iter $iter values")
//        dag.collect().foreach(println)
//        println(s"Incr res: ")
//        res.foreach(println)
//        println("----")
        res
      }
      else {
        val res = dag.collect().toSet
//        println(s"Orig res for iteration $iter:")
//        res.foreach(println)
        res
      }
    }

    val fix = tc.fixPoint(dagForIteration = (dag, iter) => dag.union(dag.join(edges).map(x => (x._2._2, x._2._1))).distinct(),
      resultForIteration = resultForIteration)

    var resultRDD: DiffRDDGenerator[(Int, Int)] = null
    var incrResultRDD: DiffRDDGenerator[(Int, Int)] = null

    time(_ => resultRDD =  fix.fixPoint(), "Orig tc")
    time(_ => incrResultRDD = fix.incrFixPoint(), "Incr tc")
//    var incrResult: Array[Diff[(Int, Int)]] = null
//    time(_ => incrResult = resultRDD.incrCollect(), "Incr tc")
    val result = resultRDD.collect()

    println("orig tc result")
    result.sorted.foreach(println)
    println("incr tc result")
    incrResultRDD.incrOutput().sorted.foreach(println)
//    incrResult.foreach(println)
  }

  def RandomTCTest(spark: SparkContext, numEdges: Int = 40, numVertices: Int = 20) =
  {
    val rand = new Random(42)

    def generateGraph = {
      val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
      while (edges.size < numEdges) {
        val from = rand.nextInt(numVertices)
        val to = rand.nextInt(numVertices)
        if (from != to) edges.+=((from, to))
      }
      edges.toSeq
    }

    val graph = generateGraph
    val removalEdge = graph.head
    println(s"Removing edge $removalEdge")

    val tc = new DiffSeqGenerator(graph, List(Removed(removalEdge)), spark)
    val edges = tc.map(x => (x._2, x._1))

    def resultForIteration(dag: DiffRDDGenerator[(Int, Int)], iter: Int, incr: Boolean) =
    {
      if(incr) { dag.incrOutput().toSet }
      else { dag.collect().toSet }
    }

//    def resultForIteration(dag: DiffRDDGenerator[(Int, Int)], iter: Int, incr: Boolean) =
//    {
//      if (iter < 20) {
//        iter % 2
//      }
//      else {
//        if (incr) { dag.incrCollect() }
//        else { dag.collect() }
//        1
//      }
//    }

    val fix = tc.fixPoint(dagForIteration = (dag, iter) => dag.union(dag.join(edges).map(x => (x._2._2, x._2._1))).distinct(),
      resultForIteration = resultForIteration)

//    val fix = tc.fixPoint(dagForIteration = (dag, iter) => dag.distinct(),
//      resultForIteration = resultForIteration)

    var resultRDD: DiffRDDGenerator[(Int, Int)] = null
    var incrResultRDD: DiffRDDGenerator[(Int, Int)] = null
    time(_ => resultRDD = fix.fixPoint(), "Orig tc")
    time(_ => incrResultRDD = fix.incrFixPoint(), "Incr tc")
//    val result = resultRDD.collect()
//    time(_ => resultRDD.incrCollect(), "Incr tc")
//    val incrResult = incrResultRDD.incrCollect()

//    println("orig tc result")
//    result.foreach(println)
//    println("incr tc result")
//    incrResult.foreach(println)
  }

//  def RandomTCTest(spark: SparkContext, numEdges: Int = 20, numVertices: Int = 10) =
//  {
//    val rand = new Random(42)
//
//    def generateGraph = {
//      val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
//      while (edges.size < numEdges) {
//        val from = rand.nextInt(numVertices)
//        val to = rand.nextInt(numVertices)
//        if (from != to) edges.+=((from, to))
//      }
//      edges.toSeq
//    }
//
//    val graph = generateGraph
//    val removalEdge = graph.head
//    println(s"Removing edge $removalEdge")
//
//    val tc = new DiffSeqGenerator(graph, List(Removed(removalEdge)), spark)
//    val edges = tc.map(x => (x._2, x._1))
//
//    var fix: DiffRDDGenerator[(Int, Int)] = tc
////    fix = fix.union(fix.join(edges).map(x => (x._2._2, x._2._1))).distinct() //1
////    fix = fix.union(fix.join(edges).map(x => (x._2._2, x._2._1))).distinct() //2
////    fix = fix.union(fix.join(edges).map(x => (x._2._2, x._2._1))).distinct() //3
////    fix = fix.union(fix.join(edges).map(x => (x._2._2, x._2._1))).distinct() //4
////    fix = fix.union(fix.join(edges).map(x => (x._2._2, x._2._1))).distinct() //5
//
//    def f(fix: DiffRDDGenerator[(Int, Int)]) = fix.distinct()
//
//    fix = f(fix)
//    fix = f(fix)
//    fix = f(fix)
//    fix = f(fix)
//    fix = f(fix)
//
//    val incrFix = fix
//
//    time(_ => fix.collect(), "Orig tc")
//    time(_ => fix.incrCollect(), "Incr tc")
//
////        println("orig tc result")
////        result.foreach(println)
////        println("incr tc result")
////        incrResult.foreach(println)
//  }

  def perfTest(spark: SparkContext): Unit =
  {
    val left = spark.makeRDD(List((1, 'a'), (2, 'b'), (3, 'c')))
    val right = spark.makeRDD(List((1, 'A'), (2, 'B'), (4, 'C')))

    val leftReduced = left.reduceByKey({(a, b) => a})
    val rightReduced = right.reduceByKey({(a, b) => a})

    val centerReduced = spark.makeRDD(List((1, 'X'), (2, 'Y'), (15, 'U'))).reduceByKey({(a, b) => a})
    centerReduced.cogroup(leftReduced, rightReduced)



    //val joined = leftReduced.join(rightReduced).join(left).join(right)
    //val joined = leftReduced.join(rightReduced.mapPartitions(w => w, true)).join(left.mapPartitions(w => w, true)).join(right.mapPartitions(w => w, true))
    /*leftReduced.mapPartitionsWithIndex({(i, iter) =>
      iter.foreach(w => println(s"($i, $w)"))
      iter
    }).collect().foreach(println)*/

    //joined.foreach(println)
  }

  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DiffTest")
    val spark = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    //incrementalFilterTest(spark)
    //deltaDataTest(spark)
    //changeFilterTest(spark)
    //joinTest(spark)
    if(args.length > 0) {
      args(0) match {
        case "filter" => changeFilterTest(spark)
        case "filter2" => changeFilterTest2(spark)
        case "filter3" => changeFilterTest3(spark)
        case "distinct" => distinctTest(spark)
        case "distinct2" => distinctTest2(spark)
        case "randomtc" => RandomTCTest(spark)
        case "tc" => TCTest(spark)
        case _ => println("Not a valid test")
      }
    }
    else {
      println("Which test should I run?")
    }
    //RandomTCTest(spark)


    //incrementalFilterTest2(spark)

    spark.stop()
  }
}