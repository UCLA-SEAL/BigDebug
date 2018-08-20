package org.apache.spark.lineage.ignite

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.apache.spark.util.PackIntIntoLong
import org.apache.spark.util.collection.CompactBuffer

import scala.language.implicitConversions

/** Global class to define PartitionWithRecId and the valid ignite cache values (corresponding to
 *  tapped lineage data
 */
object CacheDataTypes {
  val dateFormat = new SimpleDateFormat("HH:mm:ss:SSS")
  def timestampToDateStr(millis: Long) = dateFormat.format(millis)
  def setDisplayTimeZone(timeZone: TimeZone): Unit = dateFormat.setTimeZone(timeZone)
  def setDisplayTimeZone(tz: String): Unit = setDisplayTimeZone(TimeZone.getTimeZone(tz))
  setDisplayTimeZone("America/Los_Angeles") // default value
  
  /** PartitionWithRecId is a common format used throughout Titian - I've simply created a
    * wrapper to abstract out the details
    */
  case class PartitionWithRecId(value: Long) {
    def this(partition: Int, id: Int) = this(PackIntIntoLong(partition, id))
    def this(tuple: (Int, Int)) = this((PackIntIntoLong.apply _).tupled(tuple))
    def partition = PackIntIntoLong.getLeft(value)
    def split = partition // alias
    def recordId = PackIntIntoLong.getRight(value)
    def asTuple = PackIntIntoLong.extractToTuple(value)
    
    override def toString: String = asTuple.toString()
  }

  /** Base class for easy interpretation of record key in ignite.
   * As of initial implementation, every cache actually has records of (value.key, value)
   */
  abstract class CacheValue {
    def key: PartitionWithRecId
    def inputKeys: Seq[PartitionWithRecId]
    def cacheValueString: String
    
    override final def toString: String = cacheValueString
    
  }
  
  /** Cache values that correspond to tap RDDs at the end of a stage. As of
   * 8/17/2018, these are TapLRDD, TapPreShuffleLRDD, TapPreCoGroupLRDD.
   */
  abstract class EndOfStageCacheValue extends CacheValue {
    def partialLatencies: Seq[Long]
    /** Returns input IDs zipped with partial latencies. */
    def inputKeysWithPartialLatencies: Seq[(PartitionWithRecId, Long)] =
      inputKeys.zip(partialLatencies)
  }
  trait ValueWithMultipleOutputLatencies extends CacheValue {
    /** Returns the input IDs zipped with their measured latencies. This is primarily used for
     * the pre-CoGroup and pre-Shuffle RDDs.
     * In general, this is implemented by zipping inputIDs with latencies in the same
     * corresponding order.
     */
    def inputKeysWithLatencies: Seq[(PartitionWithRecId, Long)]
  }
  
  /** TapLRDD cache value */
  case class TapLRDDValue(outputId: PartitionWithRecId,
                          inputId: PartitionWithRecId,
                          latency: Long)
    extends EndOfStageCacheValue {
    
    override def key = outputId
  
    override def inputKeys: Seq[PartitionWithRecId] = Seq(inputId)
  
    override def partialLatencies: Seq[Long] = Seq(latency)
    
    override def cacheValueString = s"($outputId => $inputId, $latency)"
  }
  
  object TapLRDDValue {
    def fromRecord(r: Any) = {
      val (outputLong, inputRecId, latency) = r.asInstanceOf[(Long, Long,Long)]
      // note from jteoh:
      // In Titian, the input consists solely of the record ID as partition ID is the same and
      // the join procedure was within partitions anyways. As this is no longer the case, we
      // augment the input ID with partition after the fact. Note that this is technically
      // slightly suboptimal in that we would ideally directly modify Titian. For the time being,
      // I'm trying to preserve Titian code appearance for reference purposes. Feel free to write
      // this directly into TapLRDD in the future though, to simplify the coupling.
      // note from 8/7/2018 - the inputRecId.toInt is safe - it's actually an int originally, but
      // the buffer used in TapLRDD happens to use Long instead (which I've preserved to
      // minimalize changes)
      val output = PartitionWithRecId(outputLong)
      val split = output.split
      val inputWithSplit = new PartitionWithRecId(split, inputRecId.toInt)
      TapLRDDValue(output, inputWithSplit, latency)
    }
    def readableSchema = s"[${getClass.getSimpleName}] (OutputPartitionId, OutputRecId) " +
      "=> ((InputPartitionId,InputRecId), LatencyMs)"
  }
  
  case class TapHadoopLRDDValue(outputId: PartitionWithRecId,
                                byteOffset: Long, // in hadoop file
                                latency: Long)
    extends CacheValue {
    
    override def key = outputId
  
    override def inputKeys: Seq[PartitionWithRecId] =
      throw new UnsupportedOperationException("TapHadoopLRDDs represent the start of an execution tree and do not have other lineage input")
    
    override def cacheValueString = s"($outputId => $byteOffset, $latency)"
  }
  
  object TapHadoopLRDDValue {
    def fromRecord(r: Any) = {
      val tuple = r.asInstanceOf[(Long, Long,Long)]
      // implicitly rely on conversions to proper data types here, rather than using
      // `tupled` and using native types
      // As noted in TapHadoopLRDD, the first and second argument need to be swapped.
      TapHadoopLRDDValue(PartitionWithRecId(tuple._2), tuple._1, tuple._3)
    }
    def readableSchema = s"[${getClass.getSimpleName}] (OutputPartitionId, OutputRecId) " +
      "=> (ByteOffset, LatencyMs)"
  }
  
  // ----------- SHUFFLE VALUES start ---------
  
  // Note (jteoh): inputIds is int[] because the split is always the same as the split found in
  // outputId. (Not sure why TapLRDD decided to specify long, but that's not the case here)
  // These names might not be the most appropriate and are subject to change.
  case class TapPreShuffleLRDDValue(outputId: PartitionWithRecId,
                                    inputRecIds: Array[Int],
                                    outputRecordLatencies: List[Long])
    extends EndOfStageCacheValue {
  
    override def key: PartitionWithRecId = outputId
  
    override def inputKeys: Seq[PartitionWithRecId] =
      inputRecIds.map(new PartitionWithRecId(outputId.partition, _))
  
    override def partialLatencies: Seq[Long] = outputRecordLatencies
    
    override def cacheValueString = s"$outputId => ([${inputRecIds.mkString(",")}], " +
      s"[${outputRecordLatencies.mkString(",")}])"
  }
  
  object TapPreShuffleLRDDValue {
    def fromRecord(r: Any) = {
      val tuple = r.asInstanceOf[((Int, Int), Array[Int], List[Long], List[Long])]
      // TODO: Removed tuple._3 from case class, because that's the timestamp no longer used.
      TapPreShuffleLRDDValue(new PartitionWithRecId(tuple._1), tuple._2, tuple._4)
    }
    // input partition is always same as output
    def readableSchema = s"[${getClass.getSimpleName}] (OutputPartitionId, OutputRecId) => " +
      "([InputRecId*], [OutputLatencyMs*])"
  }
  
  // outputId: split + hashcode for key (not murmur!)
  // inputIds: inputIDs that mapped to the same key. Tentative, depending on ext sorter.
  //  7/12/2018 jteoh - these only appear to be used for their partitions right now, but it used
  // to be a long.
  // inputKeyHash: murmur hash (ideally unique) of the corresponding key
  // outputRecordLatency: how long it took within the stage to tap this record. Currently unused
  // (0).
  case class TapPostShuffleLRDDValue(outputId: PartitionWithRecId,
                                     inputIds: CompactBuffer[Long],
                                     inputKeyHash: Int,
                                     outputRecordLatency: Long
                                    )
    extends CacheValue {
    
    override def key = outputId

    // See comments above - inputIds is only used for partition.
    override def inputKeys: Seq[PartitionWithRecId] =
      // getLeft == partition
      inputIds.map(inp => new PartitionWithRecId(PackIntIntoLong.getLeft(inp), inputKeyHash))
  
    override def cacheValueString = s"$outputId => ([${inputKeys.mkString(",")}], " +
      s"$inputKeyHash, $outputRecordLatency)"
  }
  
  object TapPostShuffleLRDDValue {
    def fromRecord(r: Any) = {
      // jteoh: Looking at LineageRDD#goBack(), the CompactBuffer[Long]'s values are only used with
      // the PackIntIntoLeft.getLeft method. I haven't pinpointed where exactly the Longs here are
      // being created s.t. the left side consists of input splits though. For now we keep this
      // consistent with Titian, but it might be possible to convert this compact buffer to an Int
      // collection.
      // For exact location: search "jteoh" - it's not explicitly labeled as a
      // TapPostShuffleLRDD, but the schema is identical.
      val tuple = r.asInstanceOf[(Long, (CompactBuffer[Long], Int), Long, Long)]
      // TODO removed tuple._3 because timestamp is no longer used. Clean up the buffer.
      TapPostShuffleLRDDValue(PartitionWithRecId(tuple._1), tuple._2._1, tuple._2._2, tuple._4)
    }
    def readableSchema =
      s"[${getClass.getSimpleName}] (OutputPartitionId, OutputRecId) => ([(InputPartitionId, " +
        "InputRecId)*], InputKeyHash, OutputLatencyMs)"
  }
  
  case class TapPreCoGroupLRDDValue(outputId: PartitionWithRecId,
                                    inputRecIds: Array[Int],
                                    outputRecordLatencies: List[Long])
    extends EndOfStageCacheValue {
  
    override def key = outputId
  
    override def inputKeys: Seq[PartitionWithRecId] =
      inputRecIds.map(new PartitionWithRecId(outputId.partition, _))
    
    override def partialLatencies: Seq[Long] = outputRecordLatencies
  
    override def cacheValueString = s"$outputId => ([${inputRecIds.mkString(",")}], " +
      s"[${outputRecordLatencies.mkString(",")}])"
  
  }
  // ----------- SHUFFLE VALUES end ---------
  
  
  // ----------- COGROUP VALUES start ---------
  object TapPreCoGroupLRDDValue {
    def fromRecord(r: Any) = {
      val tuple = r.asInstanceOf[((Int, Int), Array[Int], List[Long], List[Long])]
      // jteoh: 8/7/2018 - not using latencies for cogroup
      // TODO - don't even collect tuple._3 in the RDD if this is finalized!
      TapPreCoGroupLRDDValue(new PartitionWithRecId(tuple._1), tuple._2, tuple._4)
    }
    // input partition is always same as output
    def readableSchema = s"[${getClass.getSimpleName}] (OutputPartitionId, OutputRecId) => " +
      "([InputRecId*], [OutputLatencyMs*])"
  }
  
  
  /** Essentially identical to TapPostShuffleLRDDValue */
  case class TapPostCoGroupLRDDValue(outputId: PartitionWithRecId,
                                     inputIds: CompactBuffer[Long],
                                     inputKeyHash: Int)
  extends CacheValue {
    
    override def key: PartitionWithRecId = outputId
  
    override def inputKeys: Seq[PartitionWithRecId] =
      inputIds.map(inp => new PartitionWithRecId(PackIntIntoLong.getLeft(inp),inputKeyHash))
  
    override def cacheValueString = s"$outputId => ([${inputKeys.mkString(",")}], " +
      s"$inputKeyHash)"
  }
  
  object TapPostCoGroupLRDDValue {
    def fromRecord(r: Any) = {
      val tuple = r.asInstanceOf[(Long, (CompactBuffer[Long], Int))]
      TapPostCoGroupLRDDValue(PartitionWithRecId(tuple._1), tuple._2._1, tuple._2._2)
    }
    def readableSchema = s"[${getClass.getSimpleName}] (OutputPartitionId, OutputRecId) => ([(InputPartitionId, " +
      "InputRecId)*], InputKeyHash, OutputTime, OutputLatencyMs)"
  }
  
  // ----------- COGROUP VALUES end ---------
}
