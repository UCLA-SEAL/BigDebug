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

import com.google.common.collect.ArrayListMultimap
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import net.openhft.koloboke.collect.map.hash.{HashIntObjMap, HashIntObjMaps}
import org.apache.spark.util.collection.{CompactBuffer, PrimitiveKeyOpenHashMap}
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TestMultiMap extends FunSuite {

test ("TestMultiMap") {
  val sizes: Array[Int] = Array[Int](1000, 10000, 100000, 1000000, 10000000)

  for(size <- sizes) {
    val kvs = generateKeys(size)

    testPrimitiveKeyOpenMap(kvs)
    testKolobokeMap(kvs)
    testFastUtilMap(kvs)
    testGuavaMultiMap(kvs)
  }
}

  private def generateKeys(size: Int): ArrayBuffer[(Int, Long)] = {
    val r = ThreadLocalRandom.current()

    val kvs: mutable.ArrayBuffer[(Int, Long)] = ArrayBuffer()

    var i: Long = 0
    while(i < size) {
      kvs.append((r.nextInt(), i))
      i += 1
    }

    kvs
  }

  private def testPrimitiveKeyOpenMap(kvs: ArrayBuffer[(Int, Long)]): Unit = {
    val map: PrimitiveKeyOpenHashMap[Int, CompactBuffer[Long]] =
      new PrimitiveKeyOpenHashMap
    val start = System.nanoTime()

    for((k, v) <- kvs) {
      map.changeValue(
        k,
        CompactBuffer(v),
        (old: CompactBuffer[Long]) => old += v)
    }

    println("PrimitiveKeyIncrementalValueOpenHashMap took " + (System.nanoTime() - start)/1000 +
      " to add " + kvs.length + " keys")
  }

  private def testKolobokeMap(keys: ArrayBuffer[(Int, Long)]): Unit = {
    val map: HashIntObjMap[CompactBuffer[Long]] = HashIntObjMaps.newMutableMap(2097152)
    val start = System.nanoTime()

    for((k, v) <- keys) {
      var old = map.get(k)
      if(old == null) {
        map.put(k, CompactBuffer(v))
      } else {
        map.put(k, old += v)
      }
    }

    println("Koloboke took " + (System.nanoTime() - start)/1000 +
      " to add " + keys.length + " keys")
  }

  private def testFastUtilMap(keys: ArrayBuffer[(Int, Long)]): Unit = {
    val map: Int2ObjectOpenHashMap[CompactBuffer[Long]] = new Int2ObjectOpenHashMap(2097152)
    val start = System.nanoTime()

    for((k, v) <- keys) {
      var old = map.get(k)
      if(old == null) {
        map.put(k, CompactBuffer(v))
      } else {
        map.put(k, old += v)
      }
    }

    println("Fastutil took " + (System.nanoTime() - start)/1000 +
      " to add " + keys.length + " keys")
  }

  private def testGuavaMultiMap(keys: ArrayBuffer[(Int, Long)]): Unit = {
    val map: ArrayListMultimap[Int, Long] = ArrayListMultimap.create()
    val start = System.nanoTime()

    for((k, v) <- keys) {
      var old = map.get(k)
      if(old == null) {
        map.put(k, v)
      } else {
        map.replaceValues(k, {old.add(v); old})
      }
    }

    println("Guava took " + (System.nanoTime() - start)/1000 +
      " to add " + keys.length + " keys")
  }
}
