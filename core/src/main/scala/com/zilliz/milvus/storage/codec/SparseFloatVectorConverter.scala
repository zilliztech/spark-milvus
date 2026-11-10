package com.zilliz.milvus.storage.codec

import java.nio.{ByteBuffer, ByteOrder}

import com.zilliz.milvus.storage.DataParseException

object SparseFloatVectorConverter {
  val MaxIndexExclusive = 0xffffffffL

  def decodeSparseFloatVector(
      bytes: Array[Byte]
  ): Seq[(Long, Float)] = {
    if (bytes.length % 8 != 0) {
      throw new DataParseException(
        s"Sparse vector byte length must be a multiple of 8, got ${bytes.length}"
      )
    }

    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    val entries = Seq.newBuilder[(Long, Float)]
    var previousIndex: Option[Long] = None
    while (buffer.hasRemaining) {
      val index = buffer.getInt() & 0xffffffffL
      val value = buffer.getFloat()
      if (index >= MaxIndexExclusive) {
        throw new DataParseException(
          s"Sparse vector index ($index) must be less than 2^32-1"
        )
      }
      previousIndex.foreach { previous =>
        if (index <= previous) {
          throw new DataParseException(
            s"Sparse vector indices must be strictly increasing, got $index after $previous"
          )
        }
      }
      if (value.isNaN || value.isInfinite || value < 0.0f) {
        throw new DataParseException(
          s"Sparse vector value ($value) must be finite and non-negative"
        )
      }
      entries += index -> value
      previousIndex = Some(index)
    }
    entries.result()
  }
}
