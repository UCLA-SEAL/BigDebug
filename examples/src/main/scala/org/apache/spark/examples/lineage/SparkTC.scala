/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.lineage

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

/**
 * Transitive closure on a graph.
 */
object SparkTC {
  val numEdges = 5
  val numVertices = 4
  val rand = new Random(42)

  def generateGraph = {
    val edges: mutable.Set[(Int, Int)] = mutable.Set.empty
//    while (edges.size < numEdges) {
//      val from = rand.nextInt(numVertices) + 1
//      val to = rand.nextInt(numVertices) + 1
//      if (from != to) edges.+=((from, to))
//    }
    edges.add(4,1)
    edges.add(3,1)
    edges.add(4,2)
    edges.add(2,4)
    edges.add(2,3)
    edges.toSeq
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("SparkTC")
    val sc = new SparkContext(sparkConf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(true)

    var tc = lc.parallelize(generateGraph, 2)

    // Linear transitive closure: each round grows paths by one edge,
    // by joining the graph's edges with the already-discovered paths.
    // e.g. join the path (y, z) from the TC with the edge (x, y) from
    // the graph to obtain the path (x, z).

    // Because join() joins on keys, the edges are stored in reversed order.
    val edges = tc.map(x => (x._2, x._1 + 1))

    // This join is iterated until a fixed point is reached.
    var oldCount = 0L
 //   var nextCount = tc.count()
    var count = 0
 //   do {
 //     oldCount = nextCount
//    // Perform the join, obtaining an RDD of (y, (z, x)) pairs,
//    // then project the result to obtain the new (x, z) paths.
 //     tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct()
    tc = tc.union(tc.join(edges).map(x => (x._2._2, x._2._1))).distinct()
      //nextCount = tc.count()
    tc.collect().foreach(println)
      count = count + 1
//    } while (nextCount != oldCount)

//    println("TC has " + tc.count() + " edges.")
    lc.setCaptureLineage(false)

    var linRdd = tc.getLineage()
    linRdd.collect().foreach(println)
//
//    for(i<-1 to count-1) {
    linRdd = linRdd.goBack()
    linRdd.collect().foreach(println)
    linRdd.show
    linRdd = linRdd.goBack()
    linRdd.collect().foreach(println)
    linRdd.show
    linRdd = linRdd.goBack()
    linRdd.collect().foreach(println)
    linRdd.show
    linRdd = linRdd.goBack()
    linRdd.collect().foreach(println)
    linRdd.show
    linRdd = linRdd.goBack()
    linRdd.collect().foreach(println)
    linRdd.show

    // Full trace backward
    linRdd = tc.getLineage()
    linRdd.collect.foreach(println)
    // linRdd = linRdd.filter(0)//4508
    linRdd = linRdd.goBackAll()
    linRdd.collect.foreach(println)
    //    println("Done")
    linRdd.show

    // Step by step trace backward one record
    linRdd = tc.getLineage()
    linRdd.collect().foreach(println)
    linRdd = linRdd.filter(0)
    linRdd.collect.foreach(println)
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
    linRdd.show
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
    linRdd.show

    // Full trace backward one record
    linRdd = tc.getLineage()
    linRdd.collect.foreach(println)
    linRdd = linRdd.filter(0)
    linRdd.collect.foreach(println)
    linRdd = linRdd.goBackAll()
    linRdd.collect.foreach(println)
    linRdd.show
//    }
//
//    var show = lineage.show
//    lineage = show.getLineage()
//    lineage.collect().foreach(println)
    sc.stop()
  }
}
