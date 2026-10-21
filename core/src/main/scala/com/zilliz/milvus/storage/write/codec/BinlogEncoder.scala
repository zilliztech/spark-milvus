package com.zilliz.milvus.storage.write.codec

import java.nio.{ByteBuffer, ByteOrder}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import com.zilliz.milvus.storage.codec.BinlogFormat._

/** Encodes the Milvus event envelope for stored index objects. */
object BinlogEncoder {
  private val mapper = new ObjectMapper()

  /** Wraps one payload in the envelope Milvus reads back.
    *
    * The layout is the one `BinlogCodec.parse` walks: the magic, a descriptor
    * event naming the object's collection, partition, segment and field, and
    * one more event carrying the bytes. `postHeaderLengths` is what Milvus
    * writes for every event type it knows, so a reader can skip an event it
    * does not handle (internal/core/src/storage/Event.cpp).
    */
  def envelope(
      kind: Int,
      collectionId: Long,
      partitionId: Long,
      segmentId: Long,
      fieldId: Long,
      dataType: Int,
      extras: JsonNode,
      payload: Array[Byte]
  ): Array[Byte] = {
    require(kind > 0 && kind < 8, s"Event type $kind is outside the envelope")
    require(payload != null, "An event carries a payload")
    require(
      extras != null && extras.isObject,
      "Descriptor extras are a JSON object"
    )
    val extraBytes =
      if (extras.isEmpty) Array.emptyByteArray
      else mapper.writeValueAsBytes(extras)
    // Descriptor: four ids, two timestamps and the payload's data type, then
    // one byte per event type, then the extras.
    val descriptorData = 8 * 4 + 8 + 8 + 4 + PostHeaderLengths.length + 4
    val descriptorLength = EventHeaderBytes + descriptorData + extraBytes.length
    val eventLength = EventHeaderBytes + BaseEventBytes + payload.length
    val total = 4 + descriptorLength + eventLength
    val out = ByteBuffer.allocate(total).order(ByteOrder.LITTLE_ENDIAN)
    out.putInt(Magic)
    def header(eventKind: Int, length: Int): Unit = {
      val start = out.position()
      out.putLong(0L)
      out.put(eventKind.toByte)
      out.putInt(length)
      out.putInt(start + length)
    }
    header(0, descriptorLength)
    out.putLong(collectionId)
    out.putLong(partitionId)
    out.putLong(segmentId)
    out.putLong(fieldId)
    out.putLong(0L)
    out.putLong(0L)
    out.putInt(dataType)
    PostHeaderLengths.foreach(out.put)
    out.putInt(extraBytes.length)
    out.put(extraBytes)
    header(kind, eventLength)
    out.putLong(0L)
    out.putLong(0L)
    out.put(payload)
    require(!out.hasRemaining, "The envelope was sized wrong")
    out.array()
  }

}
