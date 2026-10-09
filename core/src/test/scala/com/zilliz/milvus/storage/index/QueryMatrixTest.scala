package com.zilliz.milvus.storage.index

import java.nio.ByteOrder

import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}

/** The query side of the Knowhere contract: one group, one buffer. */
class QueryMatrixTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterEach {

  private var allocator: RootAllocator = _

  override def beforeEach(): Unit = allocator = new RootAllocator()

  override def afterEach(): Unit = allocator.close()

  private def read(matrix: QueryMatrix): java.nio.ByteBuffer =
    matrix.buffer.duplicate().order(ByteOrder.nativeOrder())

  test("float32 queries lie end to end in the buffer") {
    val layout = VectorLayout(VectorElementType.Float32, 2)
    val matrix = QueryMatrix.ofFloats(
      Seq(Array(1f, 2f), Array(3f, 4f)),
      layout,
      allocator
    )
    try {
      matrix.queries shouldBe 2
      matrix.dimension shouldBe 2
      matrix.buffer.capacity() shouldBe 16
      val buffer = read(matrix)
      (0 until 4).map(index => buffer.getFloat(index * 4)) shouldBe Seq(
        1f,
        2f,
        3f,
        4f
      )
    } finally matrix.close()
  }

  test("float16 and bfloat16 queries are packed in the field's element type") {
    Seq(
      VectorElementType.Float16 -> ((value: Float) =>
        FloatConverter.toFloat16Bytes(value)
      ),
      VectorElementType.BFloat16 -> ((value: Float) =>
        FloatConverter.toBFloat16Bytes(value)
      )
    ).foreach { case (elementType, encode) =>
      val layout = VectorLayout(elementType, 2)
      val matrix =
        QueryMatrix.ofFloats(Seq(Array(1.5f, -2.25f)), layout, allocator)
      try {
        matrix.buffer.capacity() shouldBe 4
        val buffer = read(matrix)
        val bytes = (0 until 4).map(index => buffer.get(index))
        bytes shouldBe (encode(1.5f) ++ encode(-2.25f))
      } finally matrix.close()
    }
  }

  test("int8 and binary queries arrive as the bytes they already are") {
    val int8 = VectorLayout(VectorElementType.Int8, 3)
    val matrix = QueryMatrix.ofBytes(
      Seq(Array[Byte](1, -2, 3), Array[Byte](4, 5, 6)),
      int8,
      allocator
    )
    try {
      matrix.buffer.capacity() shouldBe 6
      val buffer = read(matrix)
      (0 until 6).map(buffer.get) shouldBe Seq[Byte](1, -2, 3, 4, 5, 6)
    } finally matrix.close()

    val binary = VectorLayout(VectorElementType.Bit, 16)
    val bits = QueryMatrix.ofBytes(Seq(Array[Byte](1, 2)), binary, allocator)
    try bits.buffer.capacity() shouldBe 2
    finally bits.close()
  }

  test("a query of another dimension is refused") {
    val layout = VectorLayout(VectorElementType.Float32, 3)

    val failure = the[IllegalArgumentException] thrownBy QueryMatrix.ofFloats(
      Seq(Array(1f, 2f, 3f), Array(4f, 5f)),
      layout,
      allocator
    )

    failure.getMessage should include("Query 1 has 2 values")
  }

  test("a query with a value that is not finite is refused") {
    val layout = VectorLayout(VectorElementType.Float32, 2)

    val failure = the[IllegalArgumentException] thrownBy QueryMatrix.ofFloats(
      Seq(Array(1f, Float.NaN)),
      layout,
      allocator
    )

    failure.getMessage should include("not finite")
  }

  test("bytes and floats are not interchangeable") {
    the[IllegalArgumentException] thrownBy QueryMatrix.ofBytes(
      Seq(Array[Byte](1, 2, 3, 4)),
      VectorLayout(VectorElementType.Float32, 1),
      allocator
    )
    the[IllegalArgumentException] thrownBy QueryMatrix.ofFloats(
      Seq(Array(1f, 2f)),
      VectorLayout(VectorElementType.Int8, 2),
      allocator
    )
  }

  test("an empty group is refused") {
    the[IllegalArgumentException] thrownBy QueryMatrix.ofFloats(
      Seq.empty,
      VectorLayout(VectorElementType.Float32, 2),
      allocator
    )
  }

  test("a packed query set is what a group reads back") {
    val layout = VectorLayout(VectorElementType.Float32, 2)
    val packed = QueryMatrix.packFloats(
      Seq(Array(1f, 2f), Array(3f, 4f), Array(5f, 6f)),
      layout
    )

    packed.length shouldBe 24
    val group = QueryMatrix.ofPacked(packed, 1, 2, layout, allocator)
    try {
      group.queries shouldBe 2
      val buffer = read(group)
      (0 until 4).map(index => buffer.getFloat(index * 4)) shouldBe Seq(
        3f,
        4f,
        5f,
        6f
      )
    } finally group.close()
  }

  test("a packed byte query set keeps the field's element type") {
    val layout = VectorLayout(VectorElementType.Int8, 3)
    val packed =
      QueryMatrix.packBytes(
        Seq(Array[Byte](1, -2, 3), Array[Byte](4, 5, 6)),
        layout
      )

    packed shouldBe Array[Byte](1, -2, 3, 4, 5, 6)
    val group = QueryMatrix.ofPacked(packed, 1, 1, layout, allocator)
    try {
      val buffer = read(group)
      (0 until 3).map(buffer.get) shouldBe Seq[Byte](4, 5, 6)
    } finally group.close()
  }

  test("float16 packing and building agree byte for byte") {
    Seq(
      VectorElementType.Float16,
      VectorElementType.BFloat16,
      VectorElementType.Float32
    ).foreach { elementType =>
      val layout = VectorLayout(elementType, 3)
      val queries = Seq(Array(1.5f, -2.25f, 0.5f), Array(3f, 4f, -5f))
      val packed = QueryMatrix.packFloats(queries, layout)
      val built = QueryMatrix.ofFloats(queries, layout, allocator)
      try {
        val buffer = read(built)
        (0 until packed.length).map(buffer.get) shouldBe packed.toSeq
      } finally built.close()
    }
  }

  test("a group outside the packed query set is refused") {
    val layout = VectorLayout(VectorElementType.Float32, 2)
    val packed = QueryMatrix.packFloats(Seq(Array(1f, 2f)), layout)

    val failure = the[IllegalArgumentException] thrownBy QueryMatrix.ofPacked(
      packed,
      1,
      1,
      layout,
      allocator
    )

    failure.getMessage should include("packed query set holds 8 bytes")
    allocator.getAllocatedMemory shouldBe 0L
  }

  test("a failed pack leaves no allocation behind") {
    val layout = VectorLayout(VectorElementType.Float32, 2)

    the[IllegalArgumentException] thrownBy QueryMatrix.ofFloats(
      Seq(Array(1f, 2f), Array(3f)),
      layout,
      allocator
    )

    allocator.getAllocatedMemory shouldBe 0L
  }
}
