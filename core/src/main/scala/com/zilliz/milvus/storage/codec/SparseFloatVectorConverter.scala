package com.zilliz.milvus.storage.codec

import java.nio.{ByteBuffer, ByteOrder}

import com.zilliz.milvus.storage.DataParseException

object SparseFloatVectorConverter {
  private val MaxIndexExclusive = 0xffffffffL

  def encodeSparseFloatVector(sparse: Map[Long, Float]): Array[Byte] = {
    encodeSparseFloatVectorEntries(sparse.toSeq)
  }

  def encodeSparseFloatVectorEntries(
      entries: Seq[(Long, Float)]
  ): Array[Byte] = {
    val sortedEntries = entries.sortBy(_._1)

    val buf = ByteBuffer.allocate((4 + 4) * sortedEntries.size)
    buf.order(ByteOrder.LITTLE_ENDIAN)

    var previousIndex: Option[Long] = None
    for ((k, v) <- sortedEntries) {
      if (k < 0 || k >= MaxIndexExclusive) {
        throw new DataParseException(
          s"Sparse vector index ($k) must be non-negative and less than 2^32-1"
        )
      }
      if (previousIndex.contains(k)) {
        throw new DataParseException(
          s"Sparse vector index ($k) is duplicated"
        )
      }

      buf.putInt(k.toInt)

      if (v.isNaN || v.isInfinite || v < 0.0f) {
        throw new DataParseException(
          s"Sparse vector value ($v) must be finite and non-negative"
        )
      }

      buf.putFloat(v)
      previousIndex = Some(k)
    }

    buf.array()
  }

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
