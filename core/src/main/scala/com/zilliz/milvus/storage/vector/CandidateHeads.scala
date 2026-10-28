package com.zilliz.milvus.storage.vector

import java.lang.{Float => JavaFloat, Long => JavaLong}
import java.nio.ByteBuffer

import com.zilliz.milvus.storage.schema.MetricType

/** One current record per sorted run, with the best record at the root. This
  * heap lives only during a merge; its buffers and layout function never enter
  * the serializable aggregation state.
  */
private[vector] final class CandidateHeads(
    buffers: Array[ByteBuffer],
    metric: MetricType,
    width: (ByteBuffer, Int) => Int
) {
  private val sign = if (metric.smallerIsBetter) 1 else -1
  private val cursors = new Array[Int](buffers.length)
  private val widths = new Array[Int](buffers.length)
  private val heap = new Array[Int](buffers.length)
  private var size = 0

  private var at = 0
  while (at < buffers.length) {
    if (buffers(at).limit() > 0) {
      widths(at) = width(buffers(at), 0)
      heap(size) = at
      size += 1
    }
    at += 1
  }
  at = size / 2 - 1
  while (at >= 0) {
    siftDown(at)
    at -= 1
  }

  def nonEmpty: Boolean = size > 0
  def run: Int = heap(0)
  def offset: Int = cursors(run)
  def recordSize: Int = widths(run)

  /** Called only when the caller still needs another candidate, so the tail
    * after its kth winner remains unvisited.
    */
  def advance(): Unit = {
    val current = run
    cursors(current) += widths(current)
    if (cursors(current) == buffers(current).limit()) {
      size -= 1
      if (size > 0) heap(0) = heap(size)
    } else {
      // Validate the whole new record before any comparison reads its header.
      widths(current) = width(buffers(current), cursors(current))
    }
    if (size > 0) siftDown(0)
  }

  private def compare(left: Int, right: Int): Int =
    CandidateHeads.compare(
      sign,
      buffers(left),
      cursors(left),
      buffers(right),
      cursors(right)
    )

  private def siftDown(start: Int): Unit = {
    var parent = start
    val current = heap(parent)
    while (parent < size / 2) {
      var child = parent * 2 + 1
      if (child + 1 < size && compare(heap(child + 1), heap(child)) < 0)
        child += 1
      if (compare(heap(child), current) >= 0) {
        heap(parent) = current
        return
      }
      heap(parent) = heap(child)
      parent = child
    }
    heap(parent) = current
  }
}

private[vector] object CandidateHeads {

  /** Negative when the left record ranks first, using byte offsets in the
    * common candidate header rather than layout-specific record positions.
    */
  def compare(
      sign: Int,
      left: ByteBuffer,
      leftAt: Int,
      right: ByteBuffer,
      rightAt: Int
  ): Int = {
    val score = JavaFloat.compare(
      left.getFloat(leftAt + 8),
      right.getFloat(rightAt + 8)
    ) * sign
    if (score != 0) score
    else {
      val segment =
        JavaLong.compare(
          left.getInt(leftAt).toLong & 0xffffffffL,
          right.getInt(rightAt).toLong & 0xffffffffL
        )
      if (segment != 0) segment
      else
        JavaLong.compare(
          left.getInt(leftAt + 4).toLong & 0xffffffffL,
          right.getInt(rightAt + 4).toLong & 0xffffffffL
        )
    }
  }
}
