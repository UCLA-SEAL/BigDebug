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
      .setAppName("Simple Scala Application")
      .setCaptureLineage(true)
    val sc = new SparkContext(conf)
    val file = sc.textFilewithOffset(logFile, 2)
    val pairs = file.map(pair => pair._2.toString).flatMap(line => line.trim().split(" ")).map(word => (word, 1))
    val counts = pairs.reduceByKey(_ + _)
    counts.collect().foreach(println)

    // Get the lineage
    sc.setCaptureLineage(false)
    val back = sc.getBackwordLineage(counts).filter(r => r._1.equals(7,1,60)).tc()
    back.collect().foreach(println)
    file.filterHadoopInput(back).collect().foreach(println)
  }
}
