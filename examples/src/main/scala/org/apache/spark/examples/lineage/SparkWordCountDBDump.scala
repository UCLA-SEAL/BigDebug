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


object SparkWordCountDBDump {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
    var size = ""
    if(args.size < 2) {
      logFile = "README.md"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      size = args(1).substring(5)
    }
    conf.setAppName("WordCount-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(lineage)

    // Job
    val file = lc.textFile(logFile, 2)
    val pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word.trim(), 1))
    val counts = pairs.reduceByKey(_ + _)
    println(counts.count)
 //   println(counts.collect().mkString("\n"))

    lc.setCaptureLineage(false)

    Thread.sleep(10000)

    // Dumping to DB
//    val url="jdbc:mysql://localhost:3306"
//    val username = "root"
//    val password = "root"
//    val driver = "com.mysql.jdbc.Driver"
    //Class.forName("com.mysql.jdbc.Driver").newInstance
    var linRdd = counts.getLineage()
//    linRdd.saveAsDBTable(url, username, password, "Trace.post", driver)
//   // linRdd.lineageContext.getBackward()
//    linRdd = pairs.getLineage()
//    linRdd.saveAsDBTable(url, username, password, "Trace.pre", driver)
//    //linRdd.lineageContext.getBackward()
//    linRdd = counts.getLineage()
//    linRdd.saveAsDBTable(url, username, password, "Trace.hadoop", driver)
    linRdd.saveAsCSVFile("post-" + size)
    linRdd = pairs.getLineage()
    linRdd.saveAsCSVFile("pre-" + size)
    linRdd = file.getLineage()
    linRdd.saveAsCSVFile("hadoop-" + size)
    sc.stop()
  }
}
