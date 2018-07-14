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
  // TODO make this configurable
  dateFormat.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"))
  def timestampToDateStr(millis: Long) = dateFormat.format(millis)
  
  
  /** PartitionWithRecId is a common format used throughout Titian - I've simply created a
    * wrapper to abstract out the details
    */
  case class PartitionWithRecId(value: Long) {
    def this(partition: Int, id: Int) = this(PackIntIntoLong(partition, id))
    def partition = PackIntIntoLong.getLeft(value)
    def split = partition // alias
    def recordId = PackIntIntoLong.getRight(value)
    def asTuple = PackIntIntoLong.extractToTuple(value)
    
    override def toString: String = asTuple.toString()
  }

  /** Temp base class for easy interpretation of record key in ignite. In practice we might not
   * want to actually store the key inside the value object, so this is subject to change/removal.
   */
  abstract class CacheValue {
    def key: PartitionWithRecId
  }
  
  case class TapLRDDValue(outputId: PartitionWithRecId,
                          inputId: PartitionWithRecId,
                          latency: Long)
    extends CacheValue {
    
    def key = outputId
    
    override def toString = s"($outputId => $inputId, $latency)"
  }
  
  object TapLRDDValue {
    def fromRecord(r: Any) = {
      val tuple = r.asInstanceOf[(Long, Long,Long)]
      // implicitly rely on conversions to proper data types here, rather than using
      // `tupled` and using native types
      TapLRDDValue(tuple._1, tuple._2, tuple._3)
    }
    def readableSchema = s"[${getClass.getSimpleName}] (OutputPartitionId, OutputRecId) " +
      "=> ((InputPartitionId,InputRecId), LatencyMs)"
  }
  
  case class TapHadoopLRDDValue(outputId: PartitionWithRecId,
                                byteOffset: Long, // in hadoop file
                                latency: Long)
    extends CacheValue {
    
    def key = outputId
    
    override def toString = s"($outputId => $byteOffset, $latency)"
  }
  
  object TapHadoopLRDDValue {
    def fromRecord(r: Any) = {
      val tuple = r.asInstanceOf[(Long, Long,Long)]
      // implicitly rely on conversions to proper data types here, rather than using
      // `tupled` and using native types
      // As noted in TapHadoopLRDD, the first and second argument need to be swapped.
      TapHadoopLRDDValue(tuple._2, tuple._1, tuple._3)
    }
    def readableSchema = s"[${getClass.getSimpleName}] (OutputPartitionId, OutputRecId) " +
      "=> (ByteOffset, LatencyMs)"
  }
  
  // Note (jteoh): inputIds is int[] because the split is always the same as the split found in
  // outputId. (Not sure why TapLRDD decided to specify long, but that's not the case here)
  // These names might not be the most appropriate and are subject to change.
  case class TapPreShuffleLRDDValue(outputId: PartitionWithRecId,
                                    inputRecIds: Array[Int],
                                    outputTimestamps: List[Long],
                                    outputRecordLatencies: List[Long])
    extends CacheValue {
    
    def key = outputId
    
    def inputPartitionWithRecIds: Array[PartitionWithRecId] = {
      val partition = outputId.partition
      inputRecIds.map(new PartitionWithRecId(partition, _))
    }
  
    override def toString = s"$outputId => ([${inputRecIds.mkString(",")}], " +
      s"[${outputTimestamps.map(timestampToDateStr).mkString(",")}], [${outputRecordLatencies
        .mkString(",")}])"
  }
  
  object TapPreShuffleLRDDValue {
    def fromRecord(r: Any) = {
      val tuple = r.asInstanceOf[((Int, Int), Array[Int], List[Long], List[Long])]
      TapPreShuffleLRDDValue((PackIntIntoLong.apply _).tupled(tuple._1), tuple._2, tuple._3, tuple._4)
    }
    // input partition is always same as output
    def readableSchema = s"[${getClass.getSimpleName}] (OutputPartitionId, OutputRecId) => " +
      "([InputRecId*], [OutputTime*], [OutputLatencyMs*])"
  }
  
  // outputId: split + hashcode for key (not murmur!)
  // inputIds: inputIDs that mapped to the same key. Tentative, depending on ext sorter.
  //  7/12/2018 jteoh - these only appear to be used for their partitions right now, but it used
  // to be a long.
  // inputKeyHash: murmur hash (ideally unique) of the corresponding key
  // outputTimestamp: time tracked at calls to tap()
  // outputRecordLatency: how long it took within the stage to tap this record. Currently unused
  // (0).
  case class TapPostShuffleLRDDValue(outputId: PartitionWithRecId,
                                     inputIds: Seq[PartitionWithRecId],
                                     inputKeyHash: Int,
                                     outputTimestamp: Long,
                                     outputRecordLatency: Long
                                    )
    extends CacheValue {
    
    def key = outputId
  
    override def toString = s"$outputId => ([${inputIds
      .mkString(",")
    }], " +
      s"$inputKeyHash, ${timestampToDateStr(outputTimestamp)}, $outputRecordLatency)"
  }
  
  object TapPostShuffleLRDDValue {
    def fromRecord(r: Any) = {
      // jteoh: Looking at LineageRDD#goBack(), the CompactBuffer[Long]'s values are only used with
      // the PackIntIntoLeft.getLeft method. I haven't pinpointed where exactly the Longs here are
      // being created s.t. the left side consists of input splits though. For now we keep this
      // consistent with Titian, but it might be possible to convert this compact buffer to an Int
      // collection.
      val tuple = r.asInstanceOf[(Long, (CompactBuffer[Long], Int), Long, Long)]
      TapPostShuffleLRDDValue(tuple._1, tuple._2._1.map(PartitionWithRecId), tuple._2._2, tuple._3,
        tuple._4)
    }
    def readableSchema =
      s"[${getClass.getSimpleName}] (OutputPartitionId, OutputRecId) => ([(InputPartitionId, " +
        "InputRecId)*], InputKeyHash, OutputTime, OutputLatencyMs)"
  }
  
  implicit def longToPartitionRec(value: Long): PartitionWithRecId = PartitionWithRecId(value)
}
