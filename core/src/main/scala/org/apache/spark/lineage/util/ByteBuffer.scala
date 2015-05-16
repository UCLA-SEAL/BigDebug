package org.apache.spark.lineage.util

import scala.reflect.ClassTag

abstract class ByteBuffer[@specialized(Long, Int) V1: ClassTag,
    @specialized(Long, Int) V2: ClassTag] (var data: Array[Byte]) {
  private[spark] val pageSize = 64
  private[spark] var capacity = data.size
  private[spark] val deltaIncrease = 1024 // We increase by a factor of 64 * deltaIncrease
  private[spark] var buffer = java.nio.ByteBuffer.wrap(data)

  def put(value1: V1, value2: V2)

  def getData = data

  def position = buffer.position()

  def clear() = buffer.clear()

  private[spark] def grow() = {
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
