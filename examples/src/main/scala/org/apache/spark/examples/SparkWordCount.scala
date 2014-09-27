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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object SparkWordCount {
  def main(args: Array[String]) {
    val logFile = "README.md" // Should be some file on your system
    val conf = new SparkConf().setMaster("local[10]")
    .setAppName("Simple Scala Application").setLineage(false)
    val sc = new SparkContext(conf)
    val file = sc.textFile(logFile, 5)
    val pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word, 1))
    val counts = pairs.reduceByKey(_ + _)
    counts.collect().foreach(println)
    counts.getBackwardLineage((5,0,80)).foreach(println)
    counts.getForwardLineage((0,0,33)).foreach(println)
  }
}
