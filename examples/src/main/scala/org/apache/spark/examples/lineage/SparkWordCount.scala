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

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage._

object SparkWordCount {
  def main(args: Array[String]) {
    val logFile = "README.md"
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Simple Scala Application")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./tmp/")

    // Set the lineage context
    val lc = new LineageContext(sc)
    lc.setCaptureLineage(true)

    val file = lc.textFile(logFile, 6)
    var pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word, 1))
    var counts = pairs.reduceByKey(_ + _)
    counts.collect()

    // Get the lineage
    lc.setCaptureLineage(false)
    var lineage = lc.getLineage(counts)
  }
}
