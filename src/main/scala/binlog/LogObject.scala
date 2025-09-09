package com.zilliz.spark.connector.binlog

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.nio.{ByteBuffer, ByteOrder}
import scala.collection.mutable.ArrayBuffer

import io.milvus.grpc.schema.DataType

object EventTypeCode extends Enumeration {
  type EventTypeCode = Value

  val DescriptorEventType: EventTypeCode = Value(0)
  val InsertEventType: EventTypeCode = Value(1)
  val DeleteEventType: EventTypeCode = Value(2)
  val CreateCollectionEventType: EventTypeCode = Value(3)
  val DropCollectionEventType: EventTypeCode = Value(4)
  val CreatePartitionEventType: EventTypeCode = Value(5)
  val DropPartitionEventType: EventTypeCode = Value(6)
  val IndexFileEventType: EventTypeCode = Value(7)
  val EventTypeEnd: EventTypeCode = Value(8)
}

object Constants {
  // MagicNumber used in binlog
  val MagicNumber: Int = 0xfffabc
  val Endian: ByteOrder = ByteOrder.LITTLE_ENDIAN

  // reader
  val EmptyByteBuffer = ByteBuffer.allocate(0)

  // delete log reader
  val DeleteTimestampColumnName = "ts"
  val DeletePkTypeColumnName = "pkType"
  val DeletePkColumnName = "pk"

  // reader param constant
  val LogReaderPathParamName = "reader.path"
  val LogReaderTypeParamName = "reader.type"
  val LogReaderTypeInsert = "insert"
  val LogReaderTypeDelete = "delete"
  val LogReaderFieldIDs = "reader.field.ids"

  // s3 config
  val S3FileSystemTypeName = "s3.fs" // default: s3a://
  val S3Endpoint = "s3.endpoint"
  val S3BucketName = "s3.bucket"
  val S3RootPath = "s3.rootPath"
  val S3AccessKey = "s3.user"
  val S3SecretKey = "s3.password"
  val S3UseSSL = "s3.useSSL"
  val S3PathStyleAccess = "s3.pathStyleAccess"
  val S3MaxConnections = "s3.maxConnections"
  val S3PreloadPoolSize = "s3.preloadPoolSize"

  val TimestampFieldID = "1"

  def readMagicNumber(buffer: ByteBuffer) = {
    val num = buffer.getInt()
    if (num != MagicNumber) {
      throw new IOException(s"Invalid magic number: $num")
    }
  }
}

object EventHeader {
  // ByteBuffer bufferLittle = ByteBuffer.wrap(dataLittleEndian);
  // bufferLittle.order(ByteOrder.LITTLE_ENDIAN);
  def read(buffer: ByteBuffer): EventHeader = {
    val timestamp = buffer.getLong()
    val eventType = EventTypeCode.apply(
      buffer.get()
    )
    val eventLength = buffer.getInt()
    val nextPosition = buffer.getInt()
    EventHeader(timestamp, eventType, eventLength, nextPosition)
  }

  def getSize(): Int = {
    java.lang.Long.BYTES + java.lang.Byte.BYTES + java.lang.Integer.BYTES * 2
  }
}

case class EventHeader(
    timestamp: Long,
    eventType: EventTypeCode.EventTypeCode, // int8
    eventLength: Int,
    nextPosition: Int
) {
  override def toString: String = {
    s"EventHeader(timestamp: $timestamp, eventType: $eventType, eventLength: $eventLength, nextPosition: $nextPosition)"
  }
}

object DescriptorEventData {
  def read(
      buffer: ByteBuffer,
      bufferLength: Int
  ): DescriptorEventData = {
    val collectionID = buffer.getLong()
    val partitionID = buffer.getLong()
    val segmentID = buffer.getLong()
    val fieldID = buffer.getLong()
    val startTimestamp = buffer.getLong()
    val endTimestamp = buffer.getLong()
    val payloadDataType = DataType.fromValue(buffer.get())
    val postHeaderLengths = (0 until EventTypeCode.EventTypeEnd.id)
      .map(_ => buffer.get().toShort)
      .toArray
    val extraLength = buffer.getInt()
    val extraBytes =
      new Array[Byte](bufferLength - DescriptorEventData.getSize())
    buffer.get(extraBytes)
    DescriptorEventData(
      collectionID,
      partitionID,
      segmentID,
      fieldID,
      startTimestamp,
      endTimestamp,
      payloadDataType,
      postHeaderLengths,
      extraLength,
      extraBytes
    )
  }

  def getSize(): Int = {
    java.lang.Long.BYTES * 6 + java.lang.Integer.BYTES * 2 + java.lang.Short.BYTES * EventTypeCode.EventTypeEnd.id
  }
}

case class DescriptorEventData(
    collectionID: Long,
    partitionID: Long,
    segmentID: Long,
    fieldID: Long,
    startTimestamp: Long,
    endTimestamp: Long,
    payloadDataType: DataType,
    postHeaderLengths: Array[Short], // int8
    extraLength: Int, // int32
    extraData: Array[Byte]
) {
  override def toString: String = {
    s"DescriptorEventData(collectionID: $collectionID, partitionID: $partitionID, segmentID: $segmentID, fieldID: $fieldID, startTimestamp: $startTimestamp, endTimestamp: $endTimestamp, payloadDataType: $payloadDataType, postHeaderLengths: $postHeaderLengths, extraLength: $extraLength, extraData: $extraData)"
  }
}

case class DescriptorEvent(
    header: EventHeader,
    data: DescriptorEventData
) {
  override def toString: String = {
    s"DescriptorEvent(header: $header\n data: $data)"
  }
}

object BaseEventData {
  def read(buffer: ByteBuffer): BaseEventData = {
    val startTimestamp = buffer.getLong()
    val endTimestamp = buffer.getLong()
    BaseEventData(startTimestamp, endTimestamp)
  }

  def getSize(): Int = {
    java.lang.Long.BYTES * 2
  }
}

case class BaseEventData(
    startTimestamp: Long,
    endTimestamp: Long
)

class DeleteEventData(
    val baseEventData: BaseEventData,
    var pks: ArrayBuffer[String],
    var timestamps: ArrayBuffer[Long],
    var pkType: DataType
) {

  override def toString: String = {
    s"DeleteEventData(baseEventData: $baseEventData, pks: $pks, timestamps: $timestamps, pkType: $pkType)"
  }
}

class InsertEventData(
    val baseEventData: BaseEventData,
    var timestamp: Long,
    var dataType: DataType,
    var booleanData: ArrayBuffer[Boolean] = ArrayBuffer.empty[Boolean],
    var int8Data: ArrayBuffer[Byte] = ArrayBuffer.empty[Byte],
    var int16Data: ArrayBuffer[Short] = ArrayBuffer.empty[Short],
    var int32Data: ArrayBuffer[Int] = ArrayBuffer.empty[Int],
    var int64Data: ArrayBuffer[Long] = ArrayBuffer.empty[Long],
    var float32Data: ArrayBuffer[Float] = ArrayBuffer.empty[Float],
    var float64Data: ArrayBuffer[Double] = ArrayBuffer.empty[Double],
    var stringData: ArrayBuffer[String] = ArrayBuffer.empty[String],
    var arrayData: ArrayBuffer[Array[String]] =
      ArrayBuffer.empty[Array[String]],
    var binaryVectorData: ArrayBuffer[Array[Byte]] =
      ArrayBuffer.empty[Array[Byte]],
    var floatVectorData: ArrayBuffer[Array[Float]] =
      ArrayBuffer.empty[Array[Float]],
    var float16VectorData: ArrayBuffer[Array[Float]] =
      ArrayBuffer.empty[Array[Float]],
    var bfloat16VectorData: ArrayBuffer[Array[Float]] =
      ArrayBuffer.empty[Array[Float]],
    var int8VectorData: ArrayBuffer[Array[Byte]] =
      ArrayBuffer.empty[Array[Byte]],
    var sparseVectorData: ArrayBuffer[Map[Long, Float]] =
      ArrayBuffer.empty[Map[Long, Float]]
) {
  override def toString: String = {
    s"TypedInsertEventData(baseEventData: $baseEventData, timestamp: $timestamp, dataType: $dataType, dataSize: ${getDataSize()})"
  }

  def getDataSize(): Int = {
    dataType match {
      case DataType.Bool   => booleanData.length
      case DataType.Int8   => int8Data.length
      case DataType.Int16  => int16Data.length
      case DataType.Int32  => int32Data.length
      case DataType.Int64  => int64Data.length
      case DataType.Float  => float32Data.length
      case DataType.Double => float64Data.length
      case DataType.String | DataType.VarChar | DataType.JSON =>
        stringData.length
      case DataType.Array             => arrayData.length
      case DataType.BinaryVector      => binaryVectorData.length
      case DataType.FloatVector       => floatVectorData.length
      case DataType.Float16Vector     => float16VectorData.length
      case DataType.BFloat16Vector    => bfloat16VectorData.length
      case DataType.Int8Vector        => int8VectorData.length
      case DataType.SparseFloatVector => sparseVectorData.length
      case _                          => 0
    }
  }

  def getData(index: Int): Any = {
    // Boundary check to prevent ArrayIndexOutOfBoundsException
    val dataSize = getDataSize()
    if (index < 0 || index >= dataSize) {
      throw new IndexOutOfBoundsException(
        s"Index: $index, Size: $dataSize, DataType: $dataType"
      )
    }

    dataType match {
      case DataType.Bool   => booleanData(index)
      case DataType.Int8   => int8Data(index)
      case DataType.Int16  => int16Data(index)
      case DataType.Int32  => int32Data(index)
      case DataType.Int64  => int64Data(index)
      case DataType.Float  => float32Data(index)
      case DataType.Double => float64Data(index)
      case DataType.String | DataType.VarChar | DataType.JSON =>
        stringData(index)
      case DataType.Array             => arrayData(index)
      case DataType.BinaryVector      => binaryVectorData(index)
      case DataType.FloatVector       => floatVectorData(index)
      case DataType.Float16Vector     => float16VectorData(index)
      case DataType.BFloat16Vector    => bfloat16VectorData(index)
      case DataType.Int8Vector        => int8VectorData(index)
      case DataType.SparseFloatVector => sparseVectorData(index)
      case _                          => null
    }
  }

  def getDataString(index: Int): String = {
    dataType match {
      case DataType.Bool   => booleanData(index).toString
      case DataType.Int8   => int8Data(index).toString
      case DataType.Int16  => int16Data(index).toString
      case DataType.Int32  => int32Data(index).toString
      case DataType.Int64  => int64Data(index).toString
      case DataType.Float  => float32Data(index).toString
      case DataType.Double => float64Data(index).toString
      case DataType.String | DataType.VarChar | DataType.JSON =>
        stringData(index)
      case DataType.Array =>
        arrayData(index).map(_.toString).mkString(",")
      case DataType.BinaryVector =>
        binaryVectorData(index).map(_.toString).mkString(",")
      case DataType.FloatVector =>
        floatVectorData(index).map(_.toString).mkString(",")
      case DataType.Float16Vector =>
        float16VectorData(index).map(_.toString).mkString(",")
      case DataType.BFloat16Vector =>
        bfloat16VectorData(index).map(_.toString).mkString(",")
      case DataType.Int8Vector =>
        int8VectorData(index).map(_.toString).mkString(",")
      case DataType.SparseFloatVector =>
        sparseVectorData(index).map { case (k, v) => s"$k:$v" }.mkString(",")
      case _ => "null"
    }
  }
}
