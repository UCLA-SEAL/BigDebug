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


object SparkWordCountDistinct {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
    if(args.size < 2) {
      logFile = "README.md"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("local[2]")
//      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      conf.set("spark.kryo.referenceTracking", "false")
//      conf.set("spark.kryo.registrationRequired", "true")
//      conf.registerKryoClasses(Array(
//        classOf[RoaringBitmap],
//        classOf[BitmapContainer],
//        classOf[RoaringArray],
//        classOf[RoaringArray.Element],
//        classOf[ArrayContainer],
//        classOf[Array[RoaringArray.Element]],
//        classOf[Array[Tuple2[_, _]]],
//        classOf[Array[Short]],
//        classOf[Array[Int]],
//        classOf[Array[Long]],
//        classOf[Array[Object]]
//      ))
    }
    conf.setAppName("WordCount-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(true)
    //Seq("the", "quick")
    // Job
    val doc1 = lc.textFile("doc1", 1)
    val doc2 = lc.textFile("doc2", 1)
    val pairs1 = doc1.flatMap(line => line.trim().split(" ")).map(word => (word.trim(), "doc1"))
    val pairs2 = doc2.flatMap(line => line.trim().split(" ")).map(word => (word.trim(), "doc2"))
    val union = pairs1.union(pairs2)
    val group = union.groupByKey().filter(_._2.size <2).map(r => (r._2.head, 1))
    val counts = group.reduceByKey(_ + _)
    counts.collect().foreach(println)
  //println(counts.collect().mkString("\n"))

    lc.setCaptureLineage(false)
//
    Thread.sleep(1000)
    // Step by step full trace backward
//    var linRdd = counts.getLineage()
//    linRdd.collect.foreach(println)
//    linRdd.show
//    linRdd = linRdd.goBack()
//    linRdd.collect.foreach(println)
//    linRdd.show
//    linRdd = linRdd.goBack()
//    linRdd.collect.foreach(println)
//    linRdd.show
//
    // Full trace backward
//    for(i <- 1 to 10) {
//      var linRdd = counts.getLineage()
//      linRdd.collect //.foreach(println)
//      //    linRdd.show
// //     linRdd = linRdd.filter(4508) //4508
//      linRdd = linRdd.goBackAll()
//      linRdd.collect //.foreach(println)
//      println("Done")
//    }
//    linRdd.show
//
//    // Step by step trace backward one record
//    linRdd = counts.getLineage()
//    linRdd.collect().foreach(println)
//    linRdd.show
//    linRdd = linRdd.filter(262)
//    linRdd.collect.foreach(println)
//    linRdd.show
//    linRdd = linRdd.goBack()
//    linRdd.collect.foreach(println)
//    linRdd.show
//    linRdd = linRdd.goBack()
//    linRdd.collect.foreach(println)
//    linRdd.show
//
//    // Full trace backward one record
//    linRdd = counts.getLineage()
//    linRdd.collect.foreach(println)
//    linRdd.show
//    linRdd = linRdd.filter(262)
//    linRdd.collect.foreach(println)
//    linRdd.show
//    linRdd = linRdd.goBackAll()
//    linRdd.collect.foreach(println)
//    linRdd.show
//
//    // Step by step full trace forward
//    linRdd = file.getLineage()
//    linRdd.collect.foreach(println)
//    linRdd.show
//    linRdd = linRdd.goNext()
//    linRdd.collect.foreach(println)
//    linRdd.show
//    linRdd = linRdd.goNext()
//    linRdd.collect.foreach(println)
//    linRdd.show
//
//    // Full trace forward
//    linRdd = file.getLineage()
//    linRdd.collect.foreach(println)
//    linRdd.show
//    linRdd = linRdd.goNextAll()
//    linRdd.collect.foreach(println)
//    linRdd.show
//
//    // Step by step trace forward one record
//    linRdd = file.getLineage()
//    linRdd.collect().foreach(println)
//    linRdd.show
//    linRdd = linRdd.filter(2)
//    linRdd.collect.foreach(println)
//    linRdd.show
//    linRdd = linRdd.goNext()
//    linRdd.collect.foreach(println)
//    linRdd.show
//    linRdd = linRdd.goNext()
//    linRdd.collect.foreach(println)
//    linRdd.show

//    // Full trace forward one record
//    for(i <- 1 to 10) {
      var linRdd = counts.getLineage()
     // lc.replay(file)
      linRdd.collect
      linRdd = linRdd.filter(0).cache()
    linRdd.collect.foreach(println)
    linRdd.show()
    linRdd = linRdd.goBackAll()
      linRdd.collect.foreach(println)
      println("beforeshow")
      linRdd.show()
      println("Done1")
//     linRdd = linRdd.goBackAll()
//      linRdd.collect
//      var value = linRdd.take(1)(0)
//      println(value)
//      //linRdd = pairs.getLineage()
//      //var value = linRdd.take(1)(0)
//      //println(value)
//      linRdd = linRdd.filter(r => r == value).cache()
//      linRdd.collect
//      linRdd.show()
//      println("Done3")
//      linRdd = pairs.getLineage()
//      value = linRdd.take(1)(0)
//      println(value)
//      linRdd = linRdd.filter(r => r == value).cache()
//      linRdd.collect
//      linRdd.show()
//      println("Done2")
////    sc.unpersistAll(false)
//          linRdd = file.getLineage()
//      linRdd.collect().foreach(println)
//      linRdd = linRdd.filter(r => r == value)
//            //filter(r => r.asInstanceOf[((Any, Int), _)]._1 == value)
//          linRdd.collect().foreach(println)
////      //    linRdd.show
//          linRdd = linRdd.filter(0)
////
//          linRdd.collect//.foreach(println)
//          linRdd.show
////          linRdd = linRdd.goNextAll()
////          linRdd.collect()//.foreach(println)
//          println("Done2")
   // }
////    linRdd.show
    sc.stop()
  }
}
