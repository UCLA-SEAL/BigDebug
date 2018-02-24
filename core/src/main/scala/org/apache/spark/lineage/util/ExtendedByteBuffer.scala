package org.apache.spark.lineage.util

import scala.reflect.ClassTag
/**
 * Jason: Copied from ByteBuffer and modified to contain 3 values for performance tracking
 */
abstract class ExtendedByteBuffer[@specialized(Long, Int) V1: ClassTag,
                                      @specialized(Long, Int) V2: ClassTag,
                                      @specialized(Long, Int) V3: ClassTag]
                                      (var data: Array[Byte]) {
  private[spark] val pageSize = 64
  private[spark] var capacity = data.size
  private[spark] val deltaIncrease = 1024 // We increase by a factor of 64 * deltaIncrease
  private[spark] var buffer = java.nio.ByteBuffer.wrap(data)
  
  def put(value1: V1, value2: V2, value3: V3)
  
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
  
  def iterator: Iterator[(V1, V2, V3)]
}
