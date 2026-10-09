package com.zilliz.milvus.storage.index

import scala.collection.mutable

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{
  FieldVector,
  FixedSizeBinaryVector,
  Float4Vector
}
import org.apache.arrow.vector.complex.{FixedSizeListVector, ListVector}
import org.apache.arrow.vector.types.pojo.FieldType
import org.apache.arrow.vector.types.Types.MinorType
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}

import io.knowhere.DType

/** The conditions of vector-search.html section 2.2, on real Arrow batches. */
class KnowhereBuffersTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterEach {

  private var allocator: RootAllocator = _
  private val open = mutable.ArrayBuffer.empty[AutoCloseable]

  override def beforeEach(): Unit = allocator = new RootAllocator()

  override def afterEach(): Unit = {
    open.reverseIterator.foreach(_.close())
    open.clear()
    allocator.close()
  }

  private def track[A <: AutoCloseable](value: A): A = { open += value; value }

  private def excluded(
      vector: FieldVector,
      layout: VectorLayout
  ): (KnowhereBuffers.Base, Seq[Int]) = {
    val rows = mutable.ArrayBuffer.empty[Int]
    val base = track(
      KnowhereBuffers.base(vector, layout, allocator)(row => rows += row)
    )
    (base, rows.toSeq)
  }

  private def floats(base: KnowhereBuffers.Base, count: Int): Seq[Float] = {
    val buffer =
      base.buffer.duplicate().order(java.nio.ByteOrder.nativeOrder())
    (0 until count).map(index => buffer.getFloat(index * 4))
  }

  private def fixedSizeBinary(
      byteWidth: Int,
      values: Seq[Option[Array[Byte]]]
  ): FixedSizeBinaryVector = {
    val vector =
      track(new FixedSizeBinaryVector("vector", allocator, byteWidth))
    vector.allocateNew(values.size)
    values.zipWithIndex.foreach {
      case (Some(bytes), row) => vector.set(row, bytes)
      case (None, row)        => vector.setNull(row)
    }
    vector.setValueCount(values.size)
    vector
  }

  private def bytesOf(values: Seq[Float]): Array[Byte] = {
    val buffer = java.nio.ByteBuffer
      .allocate(values.size * 4)
      .order(java.nio.ByteOrder.nativeOrder())
    values.foreach(buffer.putFloat)
    buffer.array()
  }

  private def fixedSizeList(
      dimension: Int,
      rows: Seq[Option[Seq[Float]]]
  ): FixedSizeListVector = {
    val vector = track(
      FixedSizeListVector.empty("vector", dimension, allocator)
    )
    vector.addOrGetVector(FieldType.nullable(MinorType.FLOAT4.getType))
    vector.allocateNew()
    val child = vector.getDataVector.asInstanceOf[Float4Vector]
    rows.zipWithIndex.foreach {
      case (Some(values), row) =>
        vector.setNotNull(row)
        values.zipWithIndex.foreach { case (value, element) =>
          child.setSafe(row * dimension + element, value)
        }
      case (None, row) => vector.setNull(row)
    }
    child.setValueCount(rows.size * dimension)
    vector.setValueCount(rows.size)
    vector
  }

  private def list(rows: Seq[Seq[Float]]): ListVector = {
    val vector = track(ListVector.empty("vector", allocator))
    vector.addOrGetVector(FieldType.nullable(MinorType.FLOAT4.getType))
    vector.allocateNew()
    val child = vector.getDataVector.asInstanceOf[Float4Vector]
    var element = 0
    rows.zipWithIndex.foreach { case (values, row) =>
      vector.startNewValue(row)
      values.foreach { value =>
        child.setSafe(element, value)
        element += 1
      }
      vector.endValue(row, values.size)
    }
    child.setValueCount(element)
    vector.setValueCount(rows.size)
    vector
  }

  test("a fixed-size binary batch is handed over as it lies") {
    val layout = VectorLayout(VectorElementType.Float32, 2)
    val vector = fixedSizeBinary(
      layout.rowBytes,
      Seq(Some(bytesOf(Seq(1f, 2f))), Some(bytesOf(Seq(3f, 4f))))
    )

    val (base, rows) = excluded(vector, layout)

    base.borrowed shouldBe true
    base.rows shouldBe 2
    base.buffer.capacity() shouldBe 16
    rows shouldBe empty
    floats(base, 4) shouldBe Seq(1f, 2f, 3f, 4f)
  }

  test("a fixed-size list of floats is handed over as it lies") {
    val layout = VectorLayout(VectorElementType.Float32, 3)
    val vector =
      fixedSizeList(3, Seq(Some(Seq(1f, 2f, 3f)), Some(Seq(4f, 5f, 6f))))

    val (base, rows) = excluded(vector, layout)

    base.borrowed shouldBe true
    rows shouldBe empty
    floats(base, 6) shouldBe Seq(1f, 2f, 3f, 4f, 5f, 6f)
  }

  test(
    "a list whose offsets are the row number times the dimension is handed over"
  ) {
    val layout = VectorLayout(VectorElementType.Float32, 2)
    val vector = list(Seq(Seq(1f, 2f), Seq(3f, 4f), Seq(5f, 6f)))

    val (base, rows) = excluded(vector, layout)

    base.borrowed shouldBe true
    rows shouldBe empty
    floats(base, 6) shouldBe Seq(1f, 2f, 3f, 4f, 5f, 6f)
  }

  test("a list row with another element count fails") {
    val layout = VectorLayout(VectorElementType.Float32, 2)
    val vector = list(Seq(Seq(1f, 2f), Seq(3f, 4f, 5f), Seq(6f, 7f)))

    val failure = the[IllegalArgumentException] thrownBy excluded(
      vector,
      layout
    )

    failure.getMessage should include("has 3 elements")
  }

  test("a batch with a null row is copied and the row is excluded") {
    val layout = VectorLayout(VectorElementType.Float32, 2)
    val vector = fixedSizeBinary(
      layout.rowBytes,
      Seq(Some(bytesOf(Seq(1f, 2f))), None, Some(bytesOf(Seq(5f, 6f))))
    )

    val (base, rows) = excluded(vector, layout)

    base.borrowed shouldBe false
    base.rows shouldBe 3
    rows shouldBe Seq(1)
    floats(base, 6) shouldBe Seq(1f, 2f, 0f, 0f, 5f, 6f)
  }

  test("a null row in a fixed-size list keeps the rows after it in place") {
    val layout = VectorLayout(VectorElementType.Float32, 2)
    val vector =
      fixedSizeList(2, Seq(Some(Seq(1f, 2f)), None, Some(Seq(5f, 6f))))

    val (base, rows) = excluded(vector, layout)

    base.borrowed shouldBe false
    rows shouldBe Seq(1)
    floats(base, 6) shouldBe Seq(1f, 2f, 0f, 0f, 5f, 6f)
  }

  test("a row width other than the field's fails") {
    val layout = VectorLayout(VectorElementType.Float32, 2)
    val vector = fixedSizeBinary(12, Seq(Some(bytesOf(Seq(1f, 2f, 3f)))))

    val failure = the[IllegalArgumentException] thrownBy excluded(
      vector,
      layout
    )

    failure.getMessage should include("12 bytes per row")
  }

  test("an element type other than the field's fails") {
    val layout = VectorLayout(VectorElementType.Int8, 2)
    val vector = fixedSizeList(2, Seq(Some(Seq(1f, 2f))))

    val failure = the[IllegalArgumentException] thrownBy excluded(
      vector,
      layout
    )

    failure.getMessage should include("TinyIntVector")
  }

  test("an empty batch yields an empty buffer and excludes nothing") {
    val layout = VectorLayout(VectorElementType.Float32, 4)
    val vector = fixedSizeBinary(layout.rowBytes, Seq.empty)

    val (base, rows) = excluded(vector, layout)

    base.rows shouldBe 0
    base.buffer.capacity() shouldBe 0
    rows shouldBe empty
  }

  test("every dense element type maps onto the Knowhere type") {
    VectorLayout(VectorElementType.Float32, 2).dtype shouldBe DType.FLOAT32
    VectorLayout(VectorElementType.Float16, 2).dtype shouldBe DType.FLOAT16
    VectorLayout(VectorElementType.BFloat16, 2).dtype shouldBe DType.BFLOAT16
    VectorLayout(VectorElementType.Int8, 2).dtype shouldBe DType.INT8
    VectorLayout(VectorElementType.Bit, 8).dtype shouldBe DType.BINARY
  }

  test("a binary vector takes one byte per eight dimensions") {
    val layout = VectorLayout(VectorElementType.Bit, 16)
    layout.rowBytes shouldBe 2
    val vector = fixedSizeBinary(2, Seq(Some(Array[Byte](1, 2))))

    val (base, rows) = excluded(vector, layout)

    base.borrowed shouldBe true
    base.buffer.capacity() shouldBe 2
    rows shouldBe empty
  }
}
