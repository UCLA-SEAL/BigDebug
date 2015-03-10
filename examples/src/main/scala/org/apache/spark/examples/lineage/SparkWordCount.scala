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
    val conf = new SparkConf()
    var lineage = false
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/"
    if(args.size < 2) {
      //logFile = "../../datasets/output.txt"
      //val logFile = "../../datasets/data-MicroBenchmarks/lda_wiki1w_2"
      logFile = "README.md"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    conf.setAppName("WordCount-" + lineage + "-" + logFile)
    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)
    lc.setCaptureLineage(lineage)
    val file = lc.textFile(logFile, 2)
    val pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word, 1))
    val counts = pairs.reduceByKey(_ + _)
    counts.collect().foreach(println)
    //print(counts.count())

    // Get the lineage
    lc.setCaptureLineage(false)
    var linRdd = counts.getLineage()
    linRdd.collect.foreach(println)
//    //lineage.dumpTrace
//    linRdd = linRdd.filter(r => r.equals(0,95))
//    lin.collect.foreach(println)
//    lin.show
//    //lineage = show.getLineage()
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
//    lin.show
//    //lineage = show.getLineage()
//    //var show = lineage.show().filter(r => r.equals("(programs,1)"))
//    //show.collect.foreach(println)
//    //lineage = show.getLineage.goNext()
//    //lineage.show
    linRdd = linRdd.goBack()
    linRdd.collect.foreach(println)
    linRdd.show
//    //lineage = show.getLineage()
//    lin.collect.foreach(println)
//    lin = lin.goNext()
//    lin.collect.foreach(println)
//    lin.show
//    lin = lin.goNext()
//    lin.collect.foreach(println)
//    lin.show
    //lineage = show.getLineage()
    sc.stop()
  }
}
