package com.zilliz.milvus.storage.codec

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

/** Milvus's schemaless event format: magic, descriptor and timestamped event
  * payloads. The field widths follow internal/storage/event_data.go and
  * event_header.go; Parquet bytes come from the independent PyArrow fixtures.
  */
object BinlogFixture {
  val CollectionId: Long = 469076449917723015L
  val PartitionId: Long = 469076449917723016L
  val SegmentId: Long = 469076449917897316L
  val BuildId: Long = 469076449917967340L
  val FieldId: Long = 104L
  val Extras: String = s"""{"indexBuildID":"$BuildId","nullable":false}"""

  def parquet(name: String): Array[Byte] =
    Files.readAllBytes(
      Paths.get(s"core/src/test/data/index-codec/$name.parquet")
    )

  def encode(
      payload: Array[Byte],
      dataType: Int = 0,
      extras: String = Extras,
      eventType: Int = 7
  ): Array[Byte] = {
    val extraBytes = extras.getBytes(StandardCharsets.UTF_8)
    val descriptorLength = 17 + 64 + extraBytes.length
    val eventLength = 17 + 16 + payload.length
    val b = ByteBuffer
      .allocate(4 + descriptorLength + eventLength)
      .order(ByteOrder.LITTLE_ENDIAN)
    def header(kind: Int, length: Int): Unit = {
      val start = b.position()
      b.putLong(1L).put(kind.toByte).putInt(length).putInt(start + length)
    }
    b.putInt(0xfffabc)
    header(0, descriptorLength)
    b.putLong(CollectionId)
      .putLong(PartitionId)
      .putLong(SegmentId)
      .putLong(FieldId)
    b.putLong(1L).putLong(1L).putInt(dataType)
    b.put(52.toByte)
    (1 until 8).foreach(_ => b.put(16.toByte))
    b.putInt(extraBytes.length).put(extraBytes)
    header(eventType, eventLength)
    b.putLong(1L).putLong(1L).put(payload)
    b.array()
  }

  def intAt(bytes: Array[Byte], offset: Int, value: Int): Array[Byte] = {
    val copy = bytes.clone()
    ByteBuffer.wrap(copy).order(ByteOrder.LITTLE_ENDIAN).putInt(offset, value)
    copy
  }
}
