package com.zilliz.milvus.storage.index

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.{
  FieldVector,
  FixedSizeBinaryVector,
  Float2Vector,
  Float4Vector,
  TinyIntVector
}
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector}

import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}

import io.knowhere.DType

/** Turns one Arrow batch of a dense vector column into the base buffer Knowhere
  * reads, and reports the rows it must exclude.
  *
  * The batch is handed over as it lies when its layout already matches what
  * Knowhere expects (docs/design/architecture/vector-search.html section 2.2);
  * otherwise its rows are copied once into a buffer this object owns. Values
  * are not inspected: a non-finite value reaches Knowhere and shows up as a
  * non-finite score, which the caller rejects.
  */
object KnowhereBuffers {

  /** The base vectors of one batch. `borrowed` is false when the rows were
    * copied. Closing releases the copy; a borrowed buffer belongs to the batch.
    */
  final class Base private[index] (
      val buffer: ByteBuffer,
      val rows: Int,
      val borrowed: Boolean,
      private val owned: Option[ArrowBuf]
  ) extends AutoCloseable {
    override def close(): Unit = owned.foreach(_.close())
  }

  /** Several bases as one, so a search calls the engine over a working set
    * rather than over whatever the storage layer handed out.
    *
    * milvus-storage closes a Parquet row group at a megabyte
    * (`DEFAULT_MAX_ROW_GROUP_SIZE`, a constant), and its reader returns the
    * smallest batch any column group offers, so a 1024-dimension float vector
    * arrives 256 rows at a time however many rows the reader was asked for. A
    * distance computation over 256 base vectors reloads the whole query matrix
    * for those 256 columns; joined, one load serves them all.
    *
    * The parts are copied and then closed: what this returns owns its bytes,
    * and the peak is one part above the result rather than twice it.
    */
  def joined(
      parts: Seq[Base],
      rowBytes: Int,
      allocator: BufferAllocator
  ): Base = {
    require(parts.nonEmpty, "A joined base needs at least one part")
    require(rowBytes > 0, s"rowBytes must be positive: $rowBytes")
    if (parts.size == 1) return parts.head
    val rows = parts.map(_.rows.toLong).sum
    require(
      rows * rowBytes <= Int.MaxValue.toLong,
      s"A joined base of $rows rows exceeds what one buffer addresses"
    )
    val bytes = rows * rowBytes.toLong
    val target = allocator.buffer(math.max(bytes, 1L))
    try {
      var offset = 0L
      parts.foreach { part =>
        val length = part.rows.toLong * rowBytes.toLong
        val source = part.buffer.duplicate()
        source.position(0)
        source.limit(length.toInt)
        target.setBytes(offset, source)
        offset += length
      }
      val joinedBase =
        new Base(region(target, 0L, bytes), rows.toInt, false, Some(target))
      parts.foreach(_.close())
      joinedBase
    } catch {
      case failure: Throwable =>
        target.close()
        throw failure
    }
  }

  /** Every null row is reported through `excludeRow` before the buffer is
    * built; its bytes are zero and Knowhere never sees it as a candidate.
    */
  def base(
      vector: FieldVector,
      layout: VectorLayout,
      allocator: BufferAllocator
  )(excludeRow: Int => Unit): Base = {
    require(vector != null, "Vector batch must not be null")
    require(allocator != null, "Allocator must not be null")
    val rows = vector.getValueCount
    require(rows >= 0, s"Batch row count must not be negative: $rows")
    var nulls = false
    var row = 0
    while (row < rows) {
      if (vector.isNull(row)) {
        excludeRow(row)
        nulls = true
      }
      row += 1
    }
    val bytes = Math.multiplyExact(rows.toLong, layout.rowBytes.toLong)
    require(
      bytes <= Int.MaxValue,
      s"Vector batch of $bytes bytes exceeds one ByteBuffer"
    )
    val handedOver = if (nulls) None else borrow(vector, layout, rows, bytes)
    handedOver match {
      case Some(buffer) => new Base(buffer, rows, true, None)
      case None         => copy(vector, layout, allocator, rows, bytes)
    }
  }

  /** The conditions of section 2.2: a layout whose rows already lie end to end,
    * no nulls in this batch, the element type of the field, and a start aligned
    * to the element width.
    */
  private def borrow(
      vector: FieldVector,
      layout: VectorLayout,
      rows: Int,
      bytes: Long
  ): Option[ByteBuffer] = {
    val source: Option[ArrowBuf] = vector match {
      case fixed: FixedSizeBinaryVector =>
        require(
          fixed.getByteWidth == layout.rowBytes,
          s"Vector column has ${fixed.getByteWidth} bytes per row; the field needs ${layout.rowBytes}"
        )
        Some(fixed.getDataBuffer)
      case list: FixedSizeListVector =>
        require(
          list.getListSize == layout.dimension,
          s"Vector column has ${list.getListSize} elements per row; the field needs ${layout.dimension}"
        )
        elements(list.getDataVector, layout, rows)
      case list: ListVector =>
        if (regularOffsets(list, layout.dimension, rows))
          elements(list.getDataVector, layout, rows)
        else None
      case other =>
        throw new IllegalArgumentException(
          s"${other.getClass.getSimpleName} does not hold dense vectors"
        )
    }
    source
      .filter(buffer =>
        bytes <= buffer.capacity && buffer
          .memoryAddress() % layout.elementBytes == 0
      )
      .map(buffer => region(buffer, 0L, bytes))
  }

  /** The child of a list holds the elements end to end when it is the Arrow
    * type of this element type and has no nulls of its own.
    */
  private def elements(
      child: FieldVector,
      layout: VectorLayout,
      rows: Int
  ): Option[ArrowBuf] = {
    val expected = layout.elementType match {
      case VectorElementType.Float32 => classOf[Float4Vector]
      case VectorElementType.Float16 => classOf[Float2Vector]
      case VectorElementType.Int8    => classOf[TinyIntVector]
      case other =>
        throw new IllegalArgumentException(
          s"A list column cannot hold $other vectors; Milvus writes them as fixed-size binary"
        )
    }
    require(
      expected.isInstance(child),
      s"Vector column holds ${child.getClass.getSimpleName} elements; the field needs ${expected.getSimpleName}"
    )
    val elementCount = Math.multiplyExact(rows.toLong, layout.dimension.toLong)
    if (child.getNullCount > 0 || child.getValueCount < elementCount) None
    else Some(child.getDataBuffer)
  }

  private def regularOffsets(
      list: ListVector,
      dimension: Int,
      rows: Int
  ): Boolean = {
    val offsets = list.getOffsetBuffer
    var row = 0
    var regular = true
    while (regular && row <= rows) {
      regular = offsets.getInt(row.toLong * 4L) == row.toLong * dimension
      row += 1
    }
    regular
  }

  private def copy(
      vector: FieldVector,
      layout: VectorLayout,
      allocator: BufferAllocator,
      rows: Int,
      bytes: Long
  ): Base = {
    val target = allocator.buffer(math.max(bytes, 1L))
    try {
      target.setZero(0, target.capacity())
      var row = 0
      while (row < rows) {
        if (!vector.isNull(row)) copyRow(vector, layout, row, target)
        row += 1
      }
      new Base(region(target, 0L, bytes), rows, false, Some(target))
    } catch {
      case failure: Throwable =>
        target.close()
        throw failure
    }
  }

  private def copyRow(
      vector: FieldVector,
      layout: VectorLayout,
      row: Int,
      target: ArrowBuf
  ): Unit = {
    val destination = row.toLong * layout.rowBytes
    vector match {
      case fixed: FixedSizeBinaryVector =>
        require(
          fixed.getByteWidth == layout.rowBytes,
          s"Vector column has ${fixed.getByteWidth} bytes per row; the field needs ${layout.rowBytes}"
        )
        target.setBytes(
          destination,
          fixed.getDataBuffer,
          row.toLong * layout.rowBytes,
          layout.rowBytes
        )
      case list: FixedSizeListVector =>
        require(
          list.getListSize == layout.dimension,
          s"Vector column has ${list.getListSize} elements per row; the field needs ${layout.dimension}"
        )
        copyElements(
          list.getDataVector,
          layout,
          row.toLong * layout.dimension,
          destination,
          target
        )
      case list: ListVector =>
        val start = list.getElementStartIndex(row)
        val end = list.getElementEndIndex(row)
        require(
          end - start == layout.dimension,
          s"Vector at row $row has ${end - start} elements; the field needs ${layout.dimension}"
        )
        copyElements(
          list.getDataVector,
          layout,
          start.toLong,
          destination,
          target
        )
      case other =>
        throw new IllegalArgumentException(
          s"${other.getClass.getSimpleName} does not hold dense vectors"
        )
    }
  }

  private def copyElements(
      child: FieldVector,
      layout: VectorLayout,
      start: Long,
      destination: Long,
      target: ArrowBuf
  ): Unit = {
    var element = 0
    while (element < layout.dimension) {
      val index = (start + element).toInt
      require(
        !child.isNull(index),
        s"Vector element $index is null; a dense vector has no null elements"
      )
      layout.elementType match {
        case VectorElementType.Float32 =>
          target.setFloat(
            destination + element * 4L,
            child.asInstanceOf[Float4Vector].get(index)
          )
        case VectorElementType.Float16 =>
          target.setShort(
            destination + element * 2L,
            child.asInstanceOf[Float2Vector].get(index)
          )
        case VectorElementType.Int8 =>
          target.setByte(
            destination + element,
            child.asInstanceOf[TinyIntVector].get(index)
          )
        case other =>
          throw new IllegalArgumentException(
            s"A list column cannot hold $other vectors"
          )
      }
      element += 1
    }
  }

  private def region(buffer: ArrowBuf, offset: Long, length: Long): ByteBuffer =
    buffer.nioBuffer(offset, length.toInt).order(ByteOrder.nativeOrder())
}
