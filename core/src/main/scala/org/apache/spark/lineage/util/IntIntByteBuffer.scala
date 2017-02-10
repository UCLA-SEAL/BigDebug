package org.apache.spark.lineage.util

import org.apache.spark.util.PackIntIntoLong

class IntIntByteBuffer (data: Array[Byte]) extends ByteBuffer[Int, Int](data) {

  override def put(value1: Int, value2: Int) = {
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
