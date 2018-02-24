package org.apache.spark.lineage.util

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

import scala.reflect.ClassTag

class LongIntIntByteBuffer(data: Array[Byte]) extends ExtendedByteBuffer[Long, Int, Int](data) {
  
  override def put(value1: Long, value2: Int, value3: Int): Unit = {
    buffer.putLong(value1).putInt(value2).putInt(value3)
    if (position + 16 > capacity) grow() // long + int + int = 8 + 4 + 4
  }
  
  override def iterator: Iterator[(Long, Int, Int)] = new Iterator[(Long, Int, Int)] {
    private val curSize = position
    buffer.position(0)
    
    override def hasNext: Boolean = position < curSize
  
    override def next(): (Long, Int, Int) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      (buffer.getLong, buffer.getInt, buffer.getInt)
    }
  }
}
