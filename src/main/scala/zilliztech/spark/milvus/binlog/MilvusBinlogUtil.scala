package zilliztech.spark.milvus.binlog

import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.hadoop.util.PureJavaCrc32

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}

object MilvusBinlogUtil {
  val substringLengthForCRC = 100

  def littleEndianBinaryToFloatArray(littleEndianBytes: Array[Byte]): Array[Float] = {
    val buffer = ByteBuffer.wrap(littleEndianBytes).order(ByteOrder.LITTLE_ENDIAN)
    val floatArray = new Array[Float](littleEndianBytes.length / 4)

    var i = 0
    while (i < littleEndianBytes.length) {
      floatArray(i / 4) = buffer.getFloat(i)
      i += 4
    }

    floatArray
  }

  def binaryToFloatArray(binaryData: Array[Byte]): Array[Float] = {
    val floatArray = new Array[Float](binaryData.length / 4)
    var i = 0
    while (i < binaryData.length) {
      floatArray(i / 4) = java.nio.ByteBuffer.wrap(binaryData, i, 4).getFloat
      i += 4
    }
    floatArray
  }

  def floatToByteArray(floatValue: Float): Array[Byte] = {
    val buffer = ByteBuffer.allocate(4)
    buffer.putFloat(floatValue)
    buffer.array()
  }

  def int64ToLittleEndianBytes(value: Long): Array[Byte] = {
    val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    buffer.putLong(value)
    buffer.array()
  }

  def HashInt64(int64Value: Long): Long = {
    // val byteArr = int64ToLittleEndianBytes(int64Value)
    var hash = Murmur3_x86_32.hashLong(int64Value, 0)
    hash = hash & 0x7fffffff
    hash
  }

  def HashString(strValue: String): Long = {
    val str = if (strValue.length > substringLengthForCRC) {
      strValue.substring(0, substringLengthForCRC)
    } else {
      strValue
    }
    val bytes = str.getBytes(StandardCharsets.UTF_8)
    // Calculate CRC32 checksum
    val crc32 = new PureJavaCrc32()
    crc32.update(bytes, 0, bytes.length)
    val checksum: Long = crc32.getValue()
    checksum
  }

  def HashInt64PK2Shard(int64Value: Long, shardNum: Int): Int = {
    val hash = HashInt64(int64Value)
    (hash % shardNum).toInt
  }

  def HashVarcharPK2Shard(strValue: String, shardNum: Int): Int = {
    val hash = HashString(strValue)
    (hash % shardNum).toInt
  }

  def binaryToString(littleEndianBytes: Array[Byte]): String = {
//    val buffer = ByteBuffer.wrap(littleEndianBytes).order(ByteOrder.LITTLE_ENDIAN)
    new String(littleEndianBytes)
  }
}
