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
    if(args.size < 2) {
      logFile = "README.md"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
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

    lc.setCaptureLineage(false)

    // Dumping to DB
    val url="jdbc:mysql://localhost:3306"
    val username = "root"
    val password = "root"
    Class.forName("com.mysql.jdbc.Driver").newInstance
    var linRdd = counts.getLineage()
    linRdd.saveAsDBTable(url, username, password, "Trace.post")
    linRdd.lineageContext.getBackward()
    linRdd = linRdd.lineageContext.getCurrentLineagePosition.get
    linRdd.saveAsDBTable(url, username, password, "Trace.pre")
    linRdd.lineageContext.getBackward()
    linRdd = linRdd.lineageContext.getCurrentLineagePosition.get
    linRdd.saveAsDBTable(url, username, password, "Trace.hadoop")
    sc.stop()
  }
}
