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

import org.apache.spark.{Dependency, SparkContext}

import scala.collection.mutable.{HashMap, HashSet}
import scala.reflect.ClassTag

private[spark]
class TapPostShuffleRDD[T : ClassTag](sc: SparkContext, deps: Seq[Dependency[_]])
    extends TapRDD[T](sc, deps) {

  def addResultInfo(key: T, value: (Int, Int, Long)) = TapPostShuffleRDD.resultInfo+= (key -> value)

  def getLineage(record : Any) = {
    val delta = HashSet[(Int, Int, Long)]().empty
    val id = TapPostShuffleRDD.resultInfo.get(record)
    delta += id.get
    val result = Seq(List(id.get, record))
    transitiveClosure(delta, result)
  }

  private def transitiveClosure(delta : HashSet[_], result : Seq[List[(_)]]) : Seq[List[(_)]] = {
    val deltaPrime = HashSet[Any]()
    var resultPrime = Seq[List[(_)]]()
    delta.foreach(d => {
      getRecordInfos.get(d).getOrElse(List[(Any)]()).foreach(id => {
        deltaPrime += id
        result.foreach(r => {
          val tmp = r.head
          if(tmp.equals(d)) {
            resultPrime = resultPrime.:+(id :: r)
          }
        })
      })
    })
    if(deltaPrime.nonEmpty) {
      return transitiveClosure(deltaPrime, resultPrime)
    }
    result
  }

  override def tap(record: T) = {
    val id = (firstParent[T].id, splitId, newRecordId)
    addRecordInfo(id, tContext.currentRecordInfo)
    addResultInfo(record, id)
    //println("Tapping " + record + " with id " + id + " joins with " +
    //  tContext.currentRecordInfo)
    record
  }
}

private[spark] object TapPostShuffleRDD {

  private val resultInfo = HashMap[Any, (Int, Int, Long)]()
}