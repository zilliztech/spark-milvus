package com.zilliz.milvus.storage.write.codec

import java.nio.{ByteBuffer, ByteOrder}

import com.zilliz.milvus.storage.codec.SparseFloatVectorConverter.MaxIndexExclusive
import com.zilliz.milvus.storage.DataParseException

/** Encodes sparse vectors for stored Milvus segments. */
object SparseFloatVectorEncoder {
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

}
