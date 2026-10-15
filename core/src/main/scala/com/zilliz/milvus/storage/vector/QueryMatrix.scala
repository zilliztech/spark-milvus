package com.zilliz.milvus.storage.vector

import java.lang.{Float => JavaFloat}
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}

/** One query group, packed the way Knowhere reads a base: the queries lie end
  * to end in one buffer, in the element type of the field.
  *
  * A task builds this once and every segment it searches uses it again
  * (docs/design/projects/spark-milvus/architecture/vector-search.html section
  * 2.2).
  */
final class QueryMatrix private (
    val buffer: ByteBuffer,
    val queries: Int,
    val layout: VectorLayout,
    private val owned: ArrowBuf
) extends AutoCloseable {
  def dimension: Int = layout.dimension

  /** Each query's squared norm, in double, worked out on first use and kept for
    * every batch the group is searched against: what an L2 exact scan bounds
    * the batched entry's error with ([[ExactScan]]). Float queries only.
    */
  lazy val squaredNorms: Array[Double] = {
    val values = new Array[Float](dimension)
    Array.tabulate(queries) { at =>
      EngineRange.decode(buffer, at, layout, values)
      var sum = 0.0
      var element = 0
      while (element < values.length) {
        val value = values(element).toDouble
        sum += value * value
        element += 1
      }
      sum
    }
  }

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
      val buffer = target
        .nioBuffer(0L, queries.size * layout.rowBytes)
        .order(ByteOrder.nativeOrder())
      queries.zipWithIndex.foreach { case (query, index) =>
        checkFloats(query, index, layout)
        writeFloats(buffer, query, layout)
      }
      finished(target, queries.size, layout)
    } catch {
      case failure: Throwable =>
        target.close()
        throw failure
    }
  }

  /** Bytes, as the entry takes them for int8 fields: one row of `rowBytes`,
    * already in the field's element type.
    */
  def ofBytes(
      queries: Seq[Array[Byte]],
      layout: VectorLayout,
      allocator: BufferAllocator
  ): QueryMatrix = {
    require(queries != null && queries.nonEmpty, "A group holds no queries")
    requireByteQueries(layout)
    val target = allocate(queries.size, layout, allocator)
    try {
      queries.zipWithIndex.foreach { case (query, index) =>
        checkBytes(query, index, layout)
        target.setBytes(index.toLong * layout.rowBytes, query)
      }
      finished(target, queries.size, layout)
    } catch {
      case failure: Throwable =>
        target.close()
        throw failure
    }
  }

  /** The whole query set as one byte array, in the field's element type: what
    * the driver packs once and ships, by broadcast or with the shuffle
    * (docs/design/projects/spark-milvus/architecture/vector-search.html section
    * 2.1).
    */
  def packFloats(
      queries: Seq[Array[Float]],
      layout: VectorLayout
  ): Array[Byte] = {
    require(queries != null, "A query set must not be null")
    val packed = new Array[Byte](
      Math.multiplyExact(queries.size.toLong, layout.rowBytes.toLong).toInt
    )
    val buffer = ByteBuffer.wrap(packed).order(ByteOrder.nativeOrder())
    queries.zipWithIndex.foreach { case (query, index) =>
      checkFloats(query, index, layout)
      buffer.position(index * layout.rowBytes)
      writeFloats(buffer, query, layout)
    }
    packed
  }

  /** Byte queries, as int8 fields take them. */
  def packBytes(
      queries: Seq[Array[Byte]],
      layout: VectorLayout
  ): Array[Byte] = {
    require(queries != null, "A query set must not be null")
    requireByteQueries(layout)
    val packed = new Array[Byte](
      Math.multiplyExact(queries.size.toLong, layout.rowBytes.toLong).toInt
    )
    queries.zipWithIndex.foreach { case (query, index) =>
      checkBytes(query, index, layout)
      System.arraycopy(
        query,
        0,
        packed,
        index * layout.rowBytes,
        layout.rowBytes
      )
    }
    packed
  }

  /** One group of a packed query set, copied into the memory Knowhere reads. */
  def ofPacked(
      packed: Array[Byte],
      firstQuery: Int,
      queries: Int,
      layout: VectorLayout,
      allocator: BufferAllocator
  ): QueryMatrix = {
    require(queries > 0, s"A group holds $queries queries")
    require(firstQuery >= 0, s"A group starts at query $firstQuery")
    val from = firstQuery.toLong * layout.rowBytes
    val bytes = queries.toLong * layout.rowBytes
    require(
      packed != null && from + bytes <= packed.length,
      s"The packed query set holds ${if (packed == null) 0
        else packed.length} bytes; this group needs ${from + bytes}"
    )
    val target = allocate(queries, layout, allocator)
    try {
      target.setBytes(0L, packed, from.toInt, bytes.toInt)
      finished(target, queries, layout)
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
    allocator.buffer(bytes)
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

  private def requireByteQueries(layout: VectorLayout): Unit = require(
    layout.elementType == VectorElementType.Int8,
    s"Byte queries are for int8 fields, not for ${layout.elementType}"
  )

  private def checkFloats(
      query: Array[Float],
      index: Int,
      layout: VectorLayout
  ): Unit = {
    require(
      query != null && query.length == layout.dimension,
      s"Query $index has ${if (query == null) 0
        else query.length} values; the field needs ${layout.dimension}"
    )
    require(
      query.forall(JavaFloat.isFinite),
      s"Query $index holds a value that is not finite"
    )
  }

  private def checkBytes(
      query: Array[Byte],
      index: Int,
      layout: VectorLayout
  ): Unit = require(
    query != null && query.length == layout.rowBytes,
    s"Query $index has ${if (query == null) 0
      else query.length} bytes; the field needs ${layout.rowBytes}"
  )

  private def writeFloats(
      buffer: ByteBuffer,
      query: Array[Float],
      layout: VectorLayout
  ): Unit = layout.elementType match {
    case VectorElementType.Float32 =>
      var element = 0
      while (element < query.length) {
        buffer.putFloat(query(element))
        element += 1
      }
    case VectorElementType.Float16 =>
      var element = 0
      while (element < query.length) {
        val bits = FloatConverter.floatToFloat16Bits(query(element))
        buffer.put(bits.toByte).put((bits >>> 8).toByte)
        element += 1
      }
    case VectorElementType.BFloat16 =>
      var element = 0
      while (element < query.length) {
        val bits = FloatConverter.floatToBfloat16Bits(query(element))
        buffer.put(bits.toByte).put((bits >>> 8).toByte)
        element += 1
      }
    case other =>
      throw new IllegalArgumentException(
        s"Float queries are for float32, float16 and bfloat16 fields, not for $other"
      )
  }
}
