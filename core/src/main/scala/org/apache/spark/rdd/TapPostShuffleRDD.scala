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

package org.apache.spark.rdd

import org.apache.spark._

import scala.reflect.ClassTag

private[spark]
class TapPostShuffleRDD[T : ClassTag](sc: SparkContext, deps: Seq[Dependency[_]])
    extends TapRDD[T](sc, deps) {

  case class Schema(first: (Int, Int, Long), second: (Int, Int, Long))

  override def setCached(shuffle: ShuffledRDD[_, _, _]): TapRDD[T] = {
    cached = shuffle
    this
  }

  override def getCached = cached.setIsPostShuffleCache(true)

  override def tap(record: T) = {
    val recordId = record.asInstanceOf[Product2[T, (Int, Int, Long)]]._2
    addRecordInfo(recordId, tContext.currentRecordInfo)
    // println("Tapping " + record + " with id " + id + " joins with " +
    //   tContext.currentRecordInfo)
    //(record, recordId).asInstanceOf[T]
    record.asInstanceOf[Product2[T, (Int, Int, Long)]]._1
  }
}
