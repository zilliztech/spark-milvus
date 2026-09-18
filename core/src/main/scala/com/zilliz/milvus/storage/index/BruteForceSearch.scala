package com.zilliz.milvus.storage.index

import java.lang.{Double => JavaDouble, Float => JavaFloat, Long => JavaLong}
import java.nio.ByteOrder
import scala.collection.mutable

import org.apache.arrow.memory.{ArrowBuf, RootAllocator}

import com.zilliz.milvus.jni.vector.NativeVectorSearch

import io.knowhere.DType

/** A single-query segment search. The caller supplies vectors and copies only
  * retained row values; no caller-specific row type enters the computation.
  * Results preserve native metric scores, including squared distances for L2.
  */
final class BruteForceSearch[A](
    query: Array[Float],
    topK: Int,
    metric: String
) extends AutoCloseable {
  import BruteForceSearch._

  require(query != null && query.nonEmpty, "Query vector must not be empty")
  private val queryValues = query.clone()
  require(
    queryValues.forall(JavaFloat.isFinite),
    "Query vector must be finite"
  )
  require(topK > 0, "topK must be positive")
  private val candidates = new TopK[A](topK, metric)
  private val dimension = queryValues.length
  private val parameters = s"""{"metric_type":"$metric"}"""
  private val allocator = new RootAllocator()
  private var offset = 0L
  private var closed = false

  /** A null vector excludes the row without changing its physical offset. Other
    * invalid data fails the query. Each native call borrows one batch.
    */
  def addBatch(rows: Int)(
      vectorAt: Int => Array[Float],
      valueAt: Int => A
  ): Unit = {
    require(!closed, "Search is closed")
    require(rows >= 0, "Batch row count must not be negative")
    val nextOffset = Math.addExact(offset, rows.toLong)
    if (rows == 0) return
    val allocations = mutable.ArrayBuffer.empty[ArrowBuf]
    def buffer(bytes: Long): ArrowBuf = {
      require(bytes <= Int.MaxValue, "Vector batch exceeds ByteBuffer capacity")
      val result = allocator.buffer(bytes)
      allocations += result
      result
    }
    try {
      val baseBytes = Math.multiplyExact(rows.toLong, dimension.toLong * 4L)
      val base = buffer(baseBytes)
      base.setZero(0, baseBytes)
      val mask = buffer((rows.toLong + 7L) / 8L)
      mask.setZero(0, mask.capacity())
      var visible = 0
      var row = 0
      while (row < rows) {
        val vector = vectorAt(row)
        if (vector == null) {
          val byteIndex = row / 8
          mask.setByte(byteIndex, mask.getByte(byteIndex) | (1 << (row % 8)))
        } else {
          require(
            vector.length == dimension,
            s"Vector at row ${offset + row} has dimension ${vector.length}; expected $dimension"
          )
          var column = 0
          while (column < dimension) {
            val value = vector(column)
            require(
              JavaFloat.isFinite(value),
              s"Vector at row ${offset + row} contains a non-finite value"
            )
            base.setFloat((row.toLong * dimension + column) * 4L, value)
            column += 1
          }
          visible += 1
        }
        row += 1
      }
      if (visible > 0) {
        val count = math.min(topK, visible)
        val queryBuffer = buffer(dimension.toLong * 4L)
        queryValues.indices.foreach(i =>
          queryBuffer.setFloat(i.toLong * 4L, queryValues(i))
        )
        val ids = buffer(count.toLong * 8L)
        val scores = buffer(count.toLong * 4L)
        def bytes(value: ArrowBuf, length: Long) =
          value.nioBuffer(0, length.toInt).order(ByteOrder.nativeOrder())
        NativeVectorSearch.bruteForce(
          DType.FLOAT32,
          bytes(base, baseBytes),
          rows,
          bytes(queryBuffer, dimension.toLong * 4L),
          1,
          dimension,
          count,
          bytes(mask, (rows.toLong + 7L) / 8L),
          bytes(ids, count.toLong * 8L),
          bytes(scores, count.toLong * 4L),
          parameters
        )
        val returned = mutable.HashSet.empty[Long]
        var index = 0
        while (index < count) {
          val id = ids.getLong(index.toLong * 8L)
          if (id != -1L) {
            require(
              id >= 0 && id < rows,
              s"Knowhere returned invalid row ID $id"
            )
            require(returned.add(id), s"Knowhere returned duplicate row ID $id")
            require(
              (mask.getByte(id / 8L) & (1 << (id.toInt % 8))) == 0,
              s"Knowhere returned excluded row ID $id"
            )
            val nativeScore = scores.getFloat(index.toLong * 4L)
            require(
              JavaFloat.isFinite(nativeScore),
              "Knowhere returned a non-finite score"
            )
            require(
              metric != "L2" || nativeScore >= 0,
              "Knowhere returned a negative L2 score"
            )
            candidates.add(offset + id, nativeScore.toDouble)(valueAt(id.toInt))
          }
          index += 1
        }
      }
      offset = nextOffset
    } finally allocations.reverseIterator.foreach(_.close())
  }

  def results: Vector[Hit[A]] = {
    require(!closed, "Search is closed")
    candidates.results
  }

  private[index] def allocatedBytes: Long = allocator.getAllocatedMemory

  override def close(): Unit = if (!closed) {
    closed = true
    allocator.close()
  }
}

object BruteForceSearch {

  /** The distance field carries the native metric score without conversion. */
  final case class Hit[A](value: A, distance: Double, rowOffset: Long)

  /** Bounded merge of native batch results, independent of their input order.
    */
  private[index] final class TopK[A](k: Int, metric: String) {
    require(k > 0, "topK must be positive")
    require(
      Set("L2", "IP", "COSINE").contains(metric),
      s"Unsupported metric: $metric"
    )
    private val order = new Ordering[Hit[A]] {
      override def compare(left: Hit[A], right: Hit[A]): Int = {
        val distance = JavaDouble.compare(left.distance, right.distance)
        val result = if (metric == "L2") distance else -distance
        if (result != 0) result
        else JavaLong.compare(left.rowOffset, right.rowOffset)
      }
    }
    private val heap = mutable.PriorityQueue.empty[Hit[A]](order)

    def add(offset: Long, distance: Double)(value: => A): Unit = {
      val hit = Hit(null.asInstanceOf[A], distance, offset)
      if (heap.size < k || order.lt(hit, heap.head)) {
        val retained = Hit(value, distance, offset)
        if (heap.size == k) heap.dequeue()
        heap.enqueue(retained)
      }
    }

    def results: Vector[Hit[A]] = heap.toVector.sorted(order)
  }
}
