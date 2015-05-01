package org.apache.spark.examples

import org.apache.spark._
import org.apache.spark.HelperFunctions._
//import org.apache.spark.TransformerType._
import scala.language.existentials

import scala.collection.mutable

object RewriteTest
{
  def compareResults[T](r1: Array[T], r2: Array[T]): Boolean =
  {
    val m = new mutable.HashMap[T, Int].withDefaultValue(0)
    for(i <- r1) { m(i) += 1}
    for(i <- r2) { m(i) -= 1}
    m.forall({case (k, c) =>
      if (c != 0) {
        println(s"Did not get same result for $k")
        false
      }
      else {
        true
      }
    })
  }

  def basicTest(sc: SparkContext) =
  {
    val sourceRDD = sc.textFile("README.md").flatMap(_.trim().split(" ")).map((_, 1))
    //val sourceRDD = sc.textFile("hdfs://scai01.cs.ucla.edu:9000/clash/size-40000000000").flatMap(_.trim().split(" ")).map((_, 1))
    val root = new Source(sourceRDD, "pairs")
    val w = new WorkFlow(root)
    val mapSuffix = w.add(root.mapKey(addSuffix("_mapped"), "mapSuffix"))
    val incrValues = w.add(mapSuffix.mapValue[Int](_ + 1, _ - 1, "incrementValues"))
    val counts = w.add(incrValues.reduceByKey(_ + _, "counts"))

    val transformers = List(root, mapSuffix, incrValues, counts)
    println("Initial order")
    w.run().foreach(println)
    println("----------")

    w.inject(root.filterKey(_.length < 5, "filter"))
    w.printPlan(w.logicalPlan)
    w.printPlan(w.physicalPlan)
    w.run().foreach(println)
  }

  //just to copy paste in spark cluster shell
  def introduceAllCluster(sc: SparkContext) =
  {
    def run(w: WorkFlow) = w.run().take(40).foreach(println)

    val sourceRDD = sc.textFile("hdfs://scai01.cs.ucla.edu:9000/clash/size-20000000000").flatMap(_.trim().split(" ")).map((_, 1))
    val root = new Source(sourceRDD, "pairs")
    val w = new WorkFlow(root)
    val counts = w.add(root.reduceByKey(_ + _, "counts"))

    println("initial")
    w.printLogicalPlan()
    run(w)

    val filter = w.inject(root.filterKey(_.length < 5, "filter"))
    run(w)

    val mapGz = w.inject(filter.mapKey(addSuffix(".gz"), "mapGz"))
    run(w)

    val mapTar = w.inject(filter.mapKey(addSuffix(".tar"), "mapTar"))
    run(w)

    val filter2 = w.inject(filter.filterKey(_ != ";490"))
    run(w)
  }

  def introduceAllTest(sc: SparkContext) =
  {
    val sourceRDD = sc.textFile("README.md").flatMap(_.trim().split(" ")).map((_, 1))
    val root = new Source(sourceRDD, "pairs")
    val w = new WorkFlow(root)
    val counts = w.add(root.reduceByKey(_ + _, "counts"))

    println("initial")
    w.printLogicalPlan()
    w.run()
    //w.run().foreach(println)
    println("----------")

    println("with filter")
    val filter = w.inject(root.filterKey(_.length < 5, "filter"))
    w.printLogicalPlan()
    w.printPhysicalPlan()
    w.run()
    //w.run().foreach(println)
    println("----------")

    println("with map")
    val mapSuffix = w.inject(filter.mapKey(addSuffix("_mapped"), "mapSuffix"))
    w.printLogicalPlan()
    w.printPhysicalPlan()
    w.run()
    //incrResult.foreach(println)
    println("----------")

    println("with additional filter")
    w.inject(mapSuffix.filterKey(_ != "this", "filtered_this"))
    w.printLogicalPlan()
    w.printPhysicalPlan()
    val incrResult = w.run()
    println("----------")

    //check with the regular computation
    w.removePhysicalPlan()
    val regResult = w.run()
    println("Regular plans")
    w.printLogicalPlan()
    w.printPhysicalPlan()

    if (!compareResults(regResult.asInstanceOf[Array[Any]], incrResult.asInstanceOf[Array[Any]])) {
      regResult.foreach(println)
      println("----------")
      incrResult.foreach(println)
      println("Results don't check out")
    }
    else {
      println("Results checkout")
    }
  }

  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("Rewrite Test")
    val sc = new SparkContext(conf)

    if(args.length > 0) {
      args(0) match {
        case "basic" => basicTest(sc)
        case "incr" => introduceAllTest(sc)
        case _ => println("Not a valid test")
      }
    }
    else {
      println("Which test should I run?")
    }
  }
}
