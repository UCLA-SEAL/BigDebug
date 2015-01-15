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

object SparkWordCount {
  def main(args: Array[String]) {
    //val logFile = "README.md"
    val logFile = "small-data.txt"
    val conf = new SparkConf()
      .setMaster("local[1]")
      //.setMaster("mesos://SCAI01.CS.UCLA.EDU:5050")
      //.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
      .setAppName("Simple Scala Application")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./tmp/")
    //sc.setCheckpointDir("hdfs://scai01.cs.ucla.edu:9000/clash/tmp/spark")
    val lc = new LineageContext(sc)
    lc.setCaptureLineage(true)
    val file = lc.textFile(logFile, 2)
    val pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word, 1))
    val counts = pairs.reduceByKey(_ + _)
    //TODO Ksh
    //counts.collect().foreach(println)
    counts.collect()

    // Get the lineage
    lc.setCaptureLineage(false)
    //var lineage = counts.getLineage
    var lineage = counts.getLineage()
    lineage = lineage.filter(r => r.equals(5,1,191))
    lineage.collect.foreach(println)
    var show = lineage.show
    lineage = show.getLineage()
    lineage = lineage.goBack()
    lineage.collect.foreach(println)
    show = lineage.show
    lineage = show.getLineage()
    //var show = lineage.show().filter(r => r.equals("(programs,1)"))
    //show.collect.foreach(println)
    //lineage = show.getLineage.goNext()
    //lineage.show
    lineage = lineage.goBack()
    //TODO Ksh
    //lineage.collect.foreach(println)
    show = lineage.show
    lineage = show.getLineage()
    lineage = lineage.goNext()
    //TODO Ksh
    //lineage.collect.foreach(println)
    lineage = lineage.goNext()
    //TODO Ksh
    //lineage.collect.foreach(println)
    show = lineage.show
    lineage = show.getLineage()
  }
}
