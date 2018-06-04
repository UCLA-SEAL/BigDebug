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

package org.apache.spark.executor

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.LongAccumulator


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
 */
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends Serializable {
  private[executor] val _bytesWritten = new LongAccumulator
  private[executor] val _recordsWritten = new LongAccumulator
  private[executor] val _writeTime = new LongAccumulator
  private[executor] val _dataSerializationTime = new LongAccumulator
  private[executor] val _writeIOTime = new LongAccumulator

  /**
   * Number of bytes written for the shuffle by this task.
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * Total number of records written to the shuffle by this task.
   */
  def recordsWritten: Long = _recordsWritten.sum

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   */
  def writeTime: Long = _writeTime.sum
  
  /**
   * (jteoh) Time spent serializing data from java objects to bytes, in nanoseconds.
   * Contrast with writeTime - this is ONLY serialization and does not include IO. writeTime
   * includes IO and serialization, if applicable.
   * @deprecated - In practice, this is not sufficiently accurate. We would actually need to
   *            calculate `writeTime + dataSerializationTime - writeIOTime`, at which point it's
   *            clearer and easier to manage by merging dataSerialization into writeTime.
   */
  def dataSerializationTime: Long = _dataSerializationTime.sum
  
  /**
   * (jteoh) Time spent on IO only - this is used only for operations that are known to be
   * strictly IO (ie not including serialization/deserialization). In practice, the goal is to
   * identify serialization time by subtracting this value from write, which includes both
   * serialization and IO.
   */
  def writeIOTime: Long = _writeIOTime.sum

  private[spark] def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] def incWriteTime(v: Long): Unit = _writeTime.add(v)
  // jteoh
  private[spark] def incDataSerializationTime(v: Long): Unit = _dataSerializationTime.add(v)
  private[spark] def incWriteIOTime(v: Long): Unit = _writeIOTime.add(v)
  
  private[spark] def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] def decRecordsWritten(v: Long): Unit = {
    _recordsWritten.setValue(recordsWritten - v)
  }

  // Legacy methods for backward compatibility.
  // TODO: remove these once we make this class private.
  @deprecated("use bytesWritten instead", "2.0.0")
  def shuffleBytesWritten: Long = bytesWritten
  @deprecated("use writeTime instead", "2.0.0")
  def shuffleWriteTime: Long = writeTime
  @deprecated("use recordsWritten instead", "2.0.0")
  def shuffleRecordsWritten: Long = recordsWritten

}
