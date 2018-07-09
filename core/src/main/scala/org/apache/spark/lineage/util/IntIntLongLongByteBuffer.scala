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

class IntIntLongLongByteBuffer(data: Array[Byte]) extends ExtendedByteBuffer2[Int, Int, Long, Long](data) {

  override def put(value1: Int, value2: Int, value3: Long, value4: Long): Unit = {
    buffer.putLong(PackIntIntoLong(value1, value2)).putLong(value3).putLong(value4)
    if(position  + 24 > capacity) grow() // 4 + 4 + 8 + 8
  }

  override def iterator: Iterator[(Int, Int, Long, Long)] = new Iterator[(Int, Int, Long, Long)] {
    private val curSize = position
    buffer.position(0)

    override def hasNext: Boolean = position < curSize
    override def next(): (Int, Int, Long, Long) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val next = buffer.getLong
      (PackIntIntoLong.getLeft(next), PackIntIntoLong.getRight(next), buffer.getLong, buffer.getLong)
    }
  }
}
