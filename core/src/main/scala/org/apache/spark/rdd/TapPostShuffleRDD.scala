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

import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet}
import scala.reflect.ClassTag

private[spark]
class TapPostShuffleRDD[T : ClassTag](sc: SparkContext, deps: Seq[Dependency[_]])
    extends TapRDD[T](sc, deps) {

  //def addResultInfo(key: T, value: (Int, Int, Long)) = resultInfo+= (key -> value)

  def getLineage(record : Any, isForward : Boolean = false) = {
    val delta = HashSet[(Int, Int, Long)]().empty
    var result = mutable.HashSet[List[(_)]]()
    var joinTable : HashMap[Any, Seq[Any]] = null
    if(isForward) {
      if(!TapPostShuffleRDD.isForwardInit) {
        //TapPostShuffleRDD.initForward(getRecordInfos)
      }
      joinTable = TapPostShuffleRDD.forwardInfo
    } else {
      //joinTable = getRecordInfos
    }
    val id = joinTable.get(record)
    id.get.foreach(r => {
      delta += r.asInstanceOf[(Int, Int, Long)]
      result +=(r :: (List(record)))
    })
    transitiveClosure(joinTable, delta, result)
  }

  private def transitiveClosure(joinTable : HashMap[Any, Seq[Any]],
      delta : HashSet[_],
      result : HashSet[List[(_)]]) : HashSet[List[(_)]] = {
    val deltaPrime = HashSet[Any]()
    var resultPrime = HashSet[List[(_)]]()
    delta.foreach(d => {
      joinTable.get(d).getOrElse(List[(Any)]()).foreach(id => {
        deltaPrime += id
        result.foreach(r => {
          if(r.head.equals(d)) {
            resultPrime += (id :: r)
          }
        })
      })
    })
    if(deltaPrime.nonEmpty) {
      return transitiveClosure(joinTable, deltaPrime, resultPrime)
    }
    result
  }

  override def tap(record: T) = {
    val id = (firstParent[T].id, splitId, newRecordId)
    addRecordInfo(id, tContext.currentRecordInfo)
    // println("Tapping " + record + " with id " + id + " joins with " +
    //   tContext.currentRecordInfo)
    (record, id).asInstanceOf[T]
  }
}

private[spark] object TapPostShuffleRDD {

  //private val resultInfo = HashMap[Any, (Int, Int, Long)]()
  private var forwardInit : Boolean = false
  private val forwardInfo = HashMap[Any, Seq[Any]]()

  def isForwardInit = forwardInit

  def initForward(recordInfo : HashMap[Any, Seq[(_)]]) = {
    recordInfo.foreach(r => {
      r._2.foreach(k => {
        val value = forwardInfo.get(k)
        forwardInfo += k -> value.getOrElse(List[Any]()).+:(r._1)
      })
    })
  }
}
