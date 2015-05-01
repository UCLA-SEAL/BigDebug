package org.apache.spark

import javax.lang.model.`type`.NullType

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.language.existentials
import scala.reflect._
import scala.util.control.Breaks._

/*object TransformerType extends Enumeration
{
  type TransformerType = Value
  val ActsOnNothing, ActsOnlyOnKeys, ActsOnlyOnValues, ActsOnEverything = Value
}

import TransformerType._*/

abstract class Transformer[K1: ClassTag, V1: ClassTag, K2: ClassTag, V2: ClassTag]
  (@transient var RDD: RDD[(K2, V2)],
   @transient val name: String)
   (implicit val kt1: ClassTag[K1], val vt1: ClassTag[V1], val kt2: ClassTag[K2], val vt2: ClassTag[V2])
  extends Serializable
{
  //hack because the enumeration doesn't serialize easily in the spark shell
  val (actsOnNothing, actsOnlyOnKeys, actsOnlyOnValues, actsOnEverything) = (0, 1, 2, 3)

  type SomeTransformer = Transformer[_, _, _, _]

  def this(@transient name: String)(implicit kt1: ClassTag[K1], vt1: ClassTag[V1], kt2: ClassTag[K2], vt2: ClassTag[V2]) =
    this(null, name)

  var logical: Option[SomeTransformer] = None
  var physical: Option[SomeTransformer] = None

  //this matches things like (Any, Any) => (Any, Any) — check that
  def movable(): Boolean = kt1 == kt2 && vt1 == vt2 //(kt1.tpe == kt2.tpe) && (vt1.tpe == vt2.tpe)

  def assembleThisRDD(parentRDD: RDD[(K1, V1)]): Unit

  //return a potentially new transformer that can commute with 'other'
  //returns None if cannot be commuted
  def commuteWith[K3: ClassTag, V3: ClassTag](other: Transformer[K2, V2, K3, V3]): Option[SomeTransformer]

  def filterKey(f: K2 => Boolean, name:String="FilterKey") =
  {
    def new_f(k: K2, v: V2) = f(k)
    (this, new Filter[K2, V2](new_f, actsOnlyOnKeys, name)(kt2, vt2))
  }

  def filterValue(f: V2 => Boolean, name:String="FilterValue") =
  {
    def new_f(k: K2, v: V2) = f(v)
    (this, new Filter[K2, V2](new_f, actsOnlyOnValues, name)(kt2, vt2))
  }

  def filterKeyValue(f: (K2, V2) => Boolean, name:String = "FilterKeyValue") =
  {
    (this, new Filter[K2, V2](f, actsOnEverything, name)(kt2, vt2))
  }

  def mapKey[K3: ClassTag](fpair: (K2 => K3, K3 => K2), name:String="MapKey")
  (implicit kt3: ClassTag[K3]) =
  {
    val (f, finv) = fpair
    def new_f(k: K2, v: V2) = (f(k), v)
    def new_finv(k: K3, v: V2) = if(finv != null) (finv(k), v) else null
    (this, new Map[K2, V2, K3, V2](new_f, new_finv, actsOnlyOnKeys, name)(kt2, vt2, kt3, vt2))
  }

  def mapValue[V3: ClassTag](f: V2 => V3, finv: V3 => V2, name:String="MapValue")
  (implicit vt3: ClassTag[V3]) =
  {
    def new_f(k: K2, v: V2) = (k, f(v))
    def new_finv(k: K2, v: V3) = if(finv != null) (k, finv(v)) else null
    (this, new Map[K2, V2, K2, V3](new_f, new_finv, actsOnlyOnValues, name)(kt2, vt2, kt2, vt3))
  }

  def mapKeyValue[K3: ClassTag, V3: ClassTag](f: (K2, V2) => (K3, V3), finv: (K3, V3) => (K2, V2), name:String = "MapKeyValue")
  (implicit kt3: ClassTag[K3], vt3: ClassTag[V3]) =
    (this, new Map[K2, V2, K3, V3](f, finv, actsOnEverything, name)(kt2, vt2, kt3, vt3))

  def reduceByKey(f: (V2, V2) => V2, name:String = "ReduceByKey") =
    (this, new ReduceByKey[K2, V2](f, name)(kt2, vt2))
}

class Source[K1: ClassTag, V1: ClassTag](@transient rdd: RDD[(K1, V1)], @transient name: String)
  extends Transformer[NullType, NullType, K1, V1](rdd, name)
{
  def assembleThisRDD(parentRDD: RDD[(NullType, NullType)]) = ()
  override def commuteWith[K3: ClassTag, V3: ClassTag](other: Transformer[K1, V1, K3, V3]): Option[SomeTransformer] = None
}

class Filter[K1: ClassTag, V1: ClassTag]
  (val f: (K1, V1) => Boolean,
   //val filterType: TransformerType,
   val filterType: Int,
   @transient name: String)
  extends Transformer[K1, V1, K1, V1](name)
{
  override def assembleThisRDD(parentRDD: RDD[(K1, V1)]): Unit =
  {
    RDD = parentRDD.filter({case (k, v) => f(k, v)})
  }

  override def commuteWith[K3: ClassTag, V3: ClassTag](other: Transformer[K1, V1, K3, V3]): Option[Filter[_, _]] =
  {
    other match {
      case fk: Filter[_, _] => Some(this)
      case m: Map[K1, V1, K3, V3] =>
        if (m.finv == null) {
          None
        }
        //an optimization for the common case
        else if ((filterType == actsOnlyOnKeys && m.mapType == actsOnlyOnValues) ||
          (filterType == actsOnlyOnValues && m.mapType == actsOnlyOnKeys)) {
          Some(this)
        }
        else {
          def new_f(k: K3, v: V3) = {
            val (old_key, old_value) = m.finv(k, v)
            f(old_key, old_value)
          }
          //double check filterType
          Some(new Filter[K3, V3](new_f, filterType, name + "'"))
        }
      case rv: ReduceByKey[K1, V1] => Some(this)
    }
  }
}

class Map[K1: ClassTag, V1: ClassTag, K2: ClassTag, V2: ClassTag]
 (val f: (K1, V1) => (K2, V2),
  val finv: (K2, V2) => (K1, V1),
  //@transient val mapType: TransformerType,
  @transient val mapType: Int,
  @transient name: String)
  extends Transformer[K1, V1, K2, V2](name)
{
  override def assembleThisRDD(parentRDD: RDD[(K1, V1)]): Unit =
  {
    RDD = parentRDD.map({case (k, v) => f(k, v)})
  }

  override def commuteWith[K3: ClassTag, V3: ClassTag](other: Transformer[K2, V2, K3, V3]): Option[Map[_, _, _, _]] =
  {
    val movableSelf: Map[K2, V2, K2, V2] = this.asInstanceOf[Map[K2, V2, K2, V2]]
    other match {
      case f: Filter[K2, V2] =>
        if((f.filterType == actsOnlyOnKeys && mapType == actsOnlyOnValues) ||
          (f.filterType == actsOnlyOnValues && mapType == actsOnlyOnKeys)) Some(this) else None
      case m: Map[K2, V2, K3, V3] =>
        if (m.finv == null) None
        else {
          def new_f(k: K3, v: V3) = {
            val (old_key, old_value) = m.finv(k, v)
            val (new_key, new_value) = movableSelf.f(old_key, old_value)
            m.f(new_key, new_value)
          }
          val newMapType: Int = //TransformerType =
            if(m.mapType == actsOnEverything) actsOnEverything else this.mapType
          Some(new Map[K3, V3, K3, V3](new_f, null, newMapType, name + "'"))
        }
      case rv: ReduceByKey[K2, V2] => if(mapType == actsOnlyOnKeys) Some(this) else None
    }
  }
}

class ReduceByKey[K: ClassTag, V: ClassTag]
  (val f: (V, V) => V,
   @transient name: String)
  extends Transformer[K, V, K, V](name)
{
  override def assembleThisRDD(parentRDD: RDD[(K, V)]): Unit =
  {
    RDD = parentRDD.reduceByKey(f)
    //always caching reduced RDDs
    RDD.cache()
  }

  override def commuteWith[K3: ClassTag, V3: ClassTag](other: Transformer[K, V, K3, V3]): Option[SomeTransformer] = None
}

class WorkFlow(@transient var logicalPlan: List[Transformer[_, _, _, _]] = List())
{
  type SomeTransformer = Transformer[_, _, _, _]
  type LogicalIndex = Int
  type PhysicalIndex = Int

  @transient var physicalPlan: List[SomeTransformer] = null

  def this(source: Source[_, _]) = this(List(source))

  //doing it this way so child's type which is returned is what is expected
  //(i.e. it's not converted into a generic Transformer[K2, V2, K3, V3]
  def add[T <: SomeTransformer, U <: SomeTransformer] (pair: (T, U)): U =
  {
    assert(physicalPlan == null, "Already have a physical plan, should use inject instead")
    val (parent, child) = pair
    val parentIndex = planIndex(parent, logicalPlan)
    assert(parentIndex.isDefined, "Cannot find the parent")
    assert(parentIndex.get == logicalPlan.length - 1, "Cannot add multiple children currently")
    logicalPlan = logicalPlan :+ child
    //return the child for further chaining
    pair._2
  }

  def createPhysicalPlan() =
  {
    assert(physicalPlan == null, "Already have an existing physical plan")
    physicalPlan = logicalPlan.foldRight(List[SomeTransformer]())((b,a) => b :: a)

    //identity mapping
    for (t <- logicalPlan) {
      t.physical = Some(t)
      t.logical = Some(t)
    }

    //invalidate previous results
    //don't invalidate the source
    invalidateComputationFrom(1)
  }

  def assembleComputation() =
  {
    assert(physicalPlan.length > 0)
    assert(physicalPlan.head.isInstanceOf[Source[_, _]], "First RDD must be source")
    var currentRDD: RDD[(Any, Any)] = physicalPlan.head.RDD.asInstanceOf[RDD[(Any, Any)]]
    assert(currentRDD != null, "Source RDD cannot be null")
    for (t <- physicalPlan.drop(1)) {
      val tt = t.asInstanceOf[Transformer[Any, Any, Any, Any]]
      if (t.RDD == null) {
        tt.assembleThisRDD(currentRDD)
      }
      currentRDD = tt.RDD
    }
  }

  def planIndex(transformer: SomeTransformer, plan: List[SomeTransformer]): Option[Int] =
  {
    for((t, i) <- plan.zipWithIndex) {
      if (t == transformer) return Some(i)
    }
    println(s"Could not find ${transformer.name} in the plan")
    printPlan(plan)
    None
  }

  def logicalIndex(transformer: SomeTransformer): Option[LogicalIndex] = planIndex(transformer.logical.get, logicalPlan)
  def physicalIndex(transformer: SomeTransformer): Option[PhysicalIndex] = planIndex(transformer.physical.get, physicalPlan)

  def printPlan(plan: List[SomeTransformer]) =
  {
    plan.foreach(t => print(t.name + " -> "))
    println("done")
  }

  def printLogicalPlan() = printPlan(logicalPlan)
  def printPhysicalPlan() = printPlan(physicalPlan)

  //if we want to introduce a transformer at logical index lIndex, this function
  //returns the final transformer and the index of the final parent in the logicalPlan
  def optimizedLogicalLocation(transformer: SomeTransformer, lIndex: Int): (SomeTransformer, Int) =
  {
    var current = transformer.asInstanceOf[Transformer[Any, Any, Any, Any]]
    var currentIndex: LogicalIndex = lIndex
    breakable {
      for (t <- logicalPlan.drop(lIndex)) {
        val res = current.commuteWith(t.asInstanceOf[Transformer[Any, Any, Any, Any]])
        res match {
          case None => break()
          case Some(c) =>
            current = c.asInstanceOf[Transformer[Any, Any, Any, Any]]
            currentIndex += 1
        }
      }
    }

    (current, currentIndex)
  }

  //return the physical index of the optimized transformer to be placed at logical index lIndex
  //returns None if it cannot be placed
  def optimizedPhysicalLocation(lIndex: Int): Option[PhysicalIndex] =
  {
    //get the physical index of the parent
    val pIndex: PhysicalIndex = physicalIndex(logicalPlan(lIndex - 1)).get

    //no crisscrossing of links on either side of lIndex-pIndex link
    if (logicalPlan.drop(lIndex).exists(physicalIndex(_).get < pIndex)) return None

    //move past all continuous nodes in the physicalPlan that belong earlier in the logicalPlan
    //because we have already commuted past them
    val remaining = physicalPlan.drop(pIndex + 1).dropWhile(logicalIndex(_).get < lIndex)

    //let's say we're moving t1:
    //we cannot have: t1 t2 t3 in the physical plan where t3 t1 t2 exists in the logical plan
    //because t3's effects are not seen in the physical plan yet
    if (remaining.exists(logicalIndex(_).get < lIndex)) None
    else Some(physicalPlan.length - remaining.length)
  }

  def invalidateComputationFrom(index: Int) =
  {
    for (t <- physicalPlan.drop(index)) {
      if (t.RDD != null) {
        t.RDD.unpersist()
        t.RDD = null
      }
    }
  }

  def removePhysicalPlan() =
  {
    invalidateComputationFrom(1)
    physicalPlan = null
  }

  def run() =
  {
    if (physicalPlan == null) createPhysicalPlan()
    assembleComputation()
    physicalPlan(physicalPlan.length - 1).RDD.cache()
    physicalPlan(physicalPlan.length - 1).RDD.collect()
  }

  def insertAt[A](element: A, index: Int, list: List[A]) =
  {
    //can insert at the end of the list as well
    assert(index <= list.length)
    (list.take(index) :+ element) ++ list.drop(index)
  }

  def inject[T <: SomeTransformer, U <: SomeTransformer] (pair: (T, U)) =
  {
    val (parent, child) = pair
    val lIndex: LogicalIndex = logicalIndex(parent).get + 1
    val (optimizedTransformer, optimizedLIndex) = optimizedLogicalLocation(child, lIndex)
    //can we optimize this given our physical plan?
    val canBeOptimized = optimizedPhysicalLocation(optimizedLIndex)
    logicalPlan = insertAt(child, lIndex, logicalPlan)

    canBeOptimized match {
      case None =>
        removePhysicalPlan() //invalidate current results
        createPhysicalPlan()
      case Some(pIndex) =>
        physicalPlan = insertAt(optimizedTransformer, pIndex, physicalPlan)
        invalidateComputationFrom(pIndex + 1)
        child.physical = Some(optimizedTransformer)
        optimizedTransformer.logical = Some(child)
    }

    pair._2
  }
}

object HelperFunctions
{
  def reverse() =
  {
    ((s: String) => s.reverse, (s: String) => s.reverse)
  }

  def addSuffix(suffix: String) =
  {
    ((s: String) => s + suffix, (s: String) => s.substring(0, s.length - suffix.length))
  }

  def addPrefix(prefix: String) =
  {
    ((s: String) => prefix + s, (s: String) => s.substring(prefix.length))
  }
}