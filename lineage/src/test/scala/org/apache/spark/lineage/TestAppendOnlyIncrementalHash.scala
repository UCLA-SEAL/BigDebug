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

package org.apache.spark.lineage

import java.util.concurrent.ThreadLocalRandom

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import net.openhft.koloboke.collect.map.hash.{HashIntIntMaps, HashIntIntMap}
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TestAppendOnlyIncrementalHash extends FunSuite {

test ("TestAppendOnlyIncrementalHash") {
  val sizes: Array[Long] = Array[Long](1000, 10000, 100000, 1000000, 10000000)

  for(size <- sizes) {
    val keys = generateKeys(size)

    testPrimitiveKeyOpenMap(keys)
    testKolobokeMap(keys)
    testFastUtilMap(keys)
  }
}

  private def generateKeys(size: Long): ArrayBuffer[Int] = {
    val r = ThreadLocalRandom.current()

    val keys: mutable.ArrayBuffer[Int] = ArrayBuffer()

    var i: Long = 0L
    while(i < size) {
      keys += r.nextInt()
      i += 1
    }

    keys
  }

  private def testPrimitiveKeyOpenMap(keys: ArrayBuffer[Int]): Unit = {
    val map: PrimitiveKeyIncrementalValueOpenHashMap[Int, Int] =
      new PrimitiveKeyIncrementalValueOpenHashMap(0, (prev: Int) => prev + 1)
    val start = System.nanoTime()

    for(key <- keys) {
      map.update(key)
    }

    println("PrimitiveKeyIncrementalValueOpenHashMap took " + (System.nanoTime() - start)/1000 +
      " to add " + keys.length + " keys")
  }

  private def testKolobokeMap(keys: ArrayBuffer[Int]): Unit = {
    var recordIndex : Int = 0
    val map: HashIntIntMap = HashIntIntMaps.newMutableMap()
    val start = System.nanoTime()

    for(key <- keys) {
      var index = map.getOrDefault(key, -1)
      if(index == -1) {
        map.put(key, recordIndex)
        index = recordIndex
        recordIndex += 1
      }
    }

    println("Koloboke took " + (System.nanoTime() - start)/1000 +
      " to add " + keys.length + " keys")
  }

  private def testFastUtilMap(keys: ArrayBuffer[Int]): Unit = {
    var recordIndex : Int = 0
    val map: Int2IntOpenHashMap = new Int2IntOpenHashMap()
    map.defaultReturnValue(-1)
    val start = System.nanoTime()

    for(key <- keys) {
      var index = map.get(key)
      if(index == -1) {
        map.put(key, recordIndex)
        index = recordIndex
        recordIndex += 1
      }
    }

    println("Fastutil took " + (System.nanoTime() - start)/1000 +
      " to add " + keys.length + " keys")
  }
}
