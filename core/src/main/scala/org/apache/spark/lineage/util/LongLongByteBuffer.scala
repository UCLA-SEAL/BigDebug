package org.apache.spark.lineage.util

class LongLongByteBuffer (data: Array[Byte]) extends ByteBuffer[Long, Long](data) {

  override def put(value1: Long, value2: Long) = {
    buffer.putLong(value1).putLong(value2)
    if(position  + 12 > capacity) grow()
  }

  override def iterator: Iterator[(Long, Long)] = new Iterator[(Long, Long)] {
    private val curSize = position
    buffer.position(0)

    override def hasNext: Boolean = position < curSize
    override def next(): (Long, Long) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      (buffer.getLong, buffer.getLong)
    }
  }
}
