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

package org.apache.spark.examples

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]) {
    val logFile = "README.md"
    val conf = new SparkConf()
      .setMaster("local[2]")
      //.setMaster("mesos://SCAI01.CS.UCLA.EDU:5050")
      //.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
      .setAppName("Simple Scala Application")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./tmp/")
    //sc.setCheckpointDir("hdfs://scai01.cs.ucla.edu:9000/clash/tmp/spark")
    sc.setCaptureLineage(true)
    val file = sc.textFile(logFile, 2)
    var pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word, 1))
    var counts = pairs.reduceByKey(_ + _)
    counts.collect().foreach(println)

    // Get the lineage
    sc.setCaptureLineage(false)
    var lineage = counts.getLineage
    //var filter = file.filter(r => r.contains("Spark")).getLineage()
    lineage.collect.foreach(println)
    lineage.show
    lineage = lineage.goBack
    var show = lineage.show().filter(r => r.equals("(programs,1)"))
    show.collect.foreach(println)
    lineage = show.getLineage.goNext
    lineage.show
    lineage = file.getLineage()
    //var forw = filter.forward()
    //forw.show.foreach(println)
    /*
    var back = filter.getLineage()
    //var back = counts.filterAndGetLineage(r => r.equals("(processing,,2)"))
    back.collect().foreach(println)
    var back2 = back.backward()
    back2.collect().foreach(println)
    var back3 = back2.backward()
    back3.foreach(println)
    var forw = back3.forward()
    forw.collect().foreach(println)
    var back4 = forw.backward()
    back4.collect().foreach(println)
    var show = back4.show()
    show.collect.foreach(println)
    var lin = show.getLineage()
    lin.foreach(println)
    var forw2 = lin.forward()
    forw2.collect().foreach(println)
    var forw3 = forw2.forward()
    forw3.collect().foreach(println)
    var show2 = forw3.show()
    show2.foreach(println)
    //var back = sc.getBackwardLineage(counts).filter(r => r._1.equals(7,9,37)).tc()
    //back.collect().foreach(println)
    //val filter = file.filterHadoopInput(back)
    //filter.checkpoint()
    //filter.collect().foreach(println)
    //sc.setCaptureLineage(true)
    //pairs = filter.flatMap(line => line.trim().split(" ")).map(word => (word, 1))
    //counts = pairs.reduceByKey(_ + _)
    //counts.collect().foreach(println)
    */

    // Get the lineage
    //sc.setCaptureLineage(false)
    //back = sc.getBackwordLineage(counts).tc()
    //back.collect().foreach(println)
  }
}
