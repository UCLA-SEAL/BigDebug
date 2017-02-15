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
package org.apache.spark.lineage.util

import org.apache.spark.util.PackIntIntoLong

class IntIntByteBuffer (data: Array[Byte]) extends ByteBuffer[Int, Int](data) {

  override def put(value1: Int, value2: Int): Unit = {
    buffer.putLong(PackIntIntoLong(value1, value2))
    if(position  + 8 > capacity) grow()
  }

  override def iterator: Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    private val curSize = position
    buffer.position(0)

    override def hasNext: Boolean = position < curSize
    override def next(): (Int, Int) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val next = buffer.getLong
      (PackIntIntoLong.getLeft(next), PackIntIntoLong.getRight(next))
    }
  }
}
