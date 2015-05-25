package org.apache.spark.lineage.util

class LongIntByteBuffer (data: Array[Byte]) extends ByteBuffer[Long, Int](data) {
  override def put(value1: Long, value2: Int) = {
    buffer.putLong(value1).putInt(value2)
    if(position  + 12 > capacity) grow()
  }

  override def iterator: Iterator[(Long, Int)] = new Iterator[(Long, Int)] {
    private val curSize = position
    buffer.position(0)

    override def hasNext: Boolean = position < curSize
    override def next(): (Long, Int) = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      (buffer.getLong, buffer.getInt)
    }
  }
}
