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
class IntIntIntByteBuffer(data: Array[Byte]) extends ExtendedByteBuffer[Int, Int, Int](data) {
  
  override def put(value1: Int, value2: Int, value3: Int): Unit = {
    buffer.putInt(value1).putInt(value2).putInt(value3)
    if (position + 12 > capacity) grow() // int + int + int = 4 + 4 + 4
  }
  
  override def iterator: Iterator[(Int, Int, Int)] = new Iterator[(Int, Int, Int)] {
    private val curSize = position
    buffer.position(0)
    
    override def hasNext: Boolean = position < curSize
    
    override def next(): (Int, Int, Int) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      (buffer.getInt, buffer.getInt, buffer.getInt)
    }
  }
}
