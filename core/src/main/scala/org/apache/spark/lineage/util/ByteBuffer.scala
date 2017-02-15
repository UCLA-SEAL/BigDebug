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

abstract class ByteBuffer[@specialized(Long, Int) V1: ClassTag,
    @specialized(Long, Int) V2: ClassTag] (var data: Array[Byte]) {

  private[spark] val pageSize = 64
  private[spark] var capacity = data.size
  private[spark] val deltaIncrease = 1024 // We increase by a factor of 64 * deltaIncrease
  private[spark] var buffer = java.nio.ByteBuffer.wrap(data)

  def put(value1: V1, value2: V2)

  def getData: Array[Byte] = data

  def position: Int = buffer.position()

  def clear(): Unit = buffer.clear()

  private[spark] def grow(): Unit = {
    val newCapacity = capacity + (deltaIncrease * pageSize)
    val newArray = new Array[Byte](newCapacity)
    val pos = position
    System.arraycopy(data, 0, newArray, 0, capacity)
    data = newArray
    buffer = java.nio.ByteBuffer.wrap(data)
    buffer.position(pos)
    capacity = newCapacity
  }

  def iterator: Iterator[(V1, V2)]
}
