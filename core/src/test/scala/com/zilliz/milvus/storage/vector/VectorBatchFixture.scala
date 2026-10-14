package com.zilliz.milvus.storage.vector

import java.nio.ByteOrder
import java.util.BitSet

import org.apache.arrow.memory.BufferAllocator

/** Owned float buffers for tests of scans and source lifetimes. */
private[vector] object VectorBatchFixture {
  def floats(
      vectors: Array[Array[Float]],
      count: Int,
      dimension: Int,
      offset: Long,
      allocator: BufferAllocator
  ): VectorBatch = {
    val bytes = count.toLong * dimension * 4L
    val buffer = allocator.buffer(math.max(1L, bytes))
    try {
      buffer.setZero(0L, buffer.capacity())
      val omitted = new BitSet(count)
      vectors.take(count).zipWithIndex.foreach { case (vector, row) =>
        if (vector == null) omitted.set(row)
        else {
          require(vector.length == dimension)
          vector.indices.foreach { element =>
            buffer.setFloat(
              (row.toLong * dimension + element) * 4L,
              vector(element)
            )
          }
        }
      }
      new VectorBatch {
        val base: KnowhereBuffers.Base = new KnowhereBuffers.Base(
          buffer.nioBuffer(0L, bytes.toInt).order(ByteOrder.nativeOrder()),
          count,
          false,
          Some(buffer)
        )
        val excluded: BitSet = omitted
        val firstRow: Long = offset
        val rows: Int = count
        def close(): Unit = base.close()
      }
    } catch {
      case failure: Throwable =>
        buffer.close()
        throw failure
    }
  }
}
