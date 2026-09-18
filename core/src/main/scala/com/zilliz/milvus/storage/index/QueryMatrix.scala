package com.zilliz.milvus.storage.index

import java.lang.{Float => JavaFloat}
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}

/** One query group, packed the way Knowhere reads a base: the queries lie end
  * to end in one buffer, in the element type of the field.
  *
  * A task builds this once and every segment it searches uses it again
  * (docs/design/architecture/vector-search.html section 2.2).
  */
final class QueryMatrix private (
    val buffer: ByteBuffer,
    val queries: Int,
    val layout: VectorLayout,
    private val owned: ArrowBuf
) extends AutoCloseable {
  def dimension: Int = layout.dimension

  override def close(): Unit = owned.close()
}

object QueryMatrix {

  /** Float values, as the entry takes them for float32, float16 and bfloat16
    * fields. Every value must be finite, and a query must have the field's
    * dimension.
    */
  def ofFloats(
      queries: Seq[Array[Float]],
      layout: VectorLayout,
      allocator: BufferAllocator
  ): QueryMatrix = {
    require(queries != null && queries.nonEmpty, "A group holds no queries")
    val target = allocate(queries.size, layout, allocator)
    try {
      queries.zipWithIndex.foreach { case (query, index) =>
        require(
          query != null && query.length == layout.dimension,
          s"Query $index has ${if (query == null) 0
            else query.length} values; the field needs ${layout.dimension}"
        )
        require(
          query.forall(JavaFloat.isFinite),
          s"Query $index holds a value that is not finite"
        )
        write(target, index.toLong * layout.rowBytes, query, layout)
      }
      finished(target, queries.size, layout)
    } catch {
      case failure: Throwable =>
        target.close()
        throw failure
    }
  }

  /** Bytes, as the entry takes them for int8 and binary fields: one row of
    * `rowBytes`, already in the field's element type.
    */
  def ofBytes(
      queries: Seq[Array[Byte]],
      layout: VectorLayout,
      allocator: BufferAllocator
  ): QueryMatrix = {
    require(queries != null && queries.nonEmpty, "A group holds no queries")
    require(
      layout.elementType == VectorElementType.Int8 ||
        layout.elementType == VectorElementType.Bit,
      s"${layout.elementType} queries arrive as floats, not as bytes"
    )
    val target = allocate(queries.size, layout, allocator)
    try {
      queries.zipWithIndex.foreach { case (query, index) =>
        require(
          query != null && query.length == layout.rowBytes,
          s"Query $index has ${if (query == null) 0
            else query.length} bytes; the field needs ${layout.rowBytes}"
        )
        target.setBytes(index.toLong * layout.rowBytes, query)
      }
      finished(target, queries.size, layout)
    } catch {
      case failure: Throwable =>
        target.close()
        throw failure
    }
  }

  private def allocate(
      queries: Int,
      layout: VectorLayout,
      allocator: BufferAllocator
  ): ArrowBuf = {
    require(allocator != null, "Allocator must not be null")
    val bytes = Math.multiplyExact(queries.toLong, layout.rowBytes.toLong)
    require(
      bytes <= Int.MaxValue,
      s"A query group of $bytes bytes exceeds one ByteBuffer"
    )
    val target = allocator.buffer(bytes)
    target.setZero(0, bytes)
    target
  }

  private def finished(
      target: ArrowBuf,
      queries: Int,
      layout: VectorLayout
  ): QueryMatrix = {
    val bytes = queries.toLong * layout.rowBytes
    val buffer =
      target.nioBuffer(0, bytes.toInt).order(ByteOrder.nativeOrder())
    new QueryMatrix(buffer, queries, layout, target)
  }

  private def write(
      target: ArrowBuf,
      destination: Long,
      query: Array[Float],
      layout: VectorLayout
  ): Unit = layout.elementType match {
    case VectorElementType.Float32 =>
      var element = 0
      while (element < layout.dimension) {
        target.setFloat(destination + element * 4L, query(element))
        element += 1
      }
    case VectorElementType.Float16 =>
      var element = 0
      while (element < layout.dimension) {
        val bytes = FloatConverter.toFloat16Bytes(query(element))
        target.setByte(destination + element * 2L, bytes.head)
        target.setByte(destination + element * 2L + 1L, bytes(1))
        element += 1
      }
    case VectorElementType.BFloat16 =>
      var element = 0
      while (element < layout.dimension) {
        val bytes = FloatConverter.toBFloat16Bytes(query(element))
        target.setByte(destination + element * 2L, bytes.head)
        target.setByte(destination + element * 2L + 1L, bytes(1))
        element += 1
      }
    case other =>
      throw new IllegalArgumentException(
        s"$other queries arrive as bytes, not as floats"
      )
  }
}
