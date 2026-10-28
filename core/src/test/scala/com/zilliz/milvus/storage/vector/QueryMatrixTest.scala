package com.zilliz.milvus.storage.vector

import java.lang.{Float => JavaFloat}
import java.nio.ByteOrder

import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.milvus.storage.DataParseException

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

  test("half query encodings preserve boundaries, signed zero and byte order") {
    val halfSubnormalTie = Math.scalb(1f, -25)
    Seq(
      (
        VectorElementType.Float16,
        FloatConverter.floatToFloat16Bits _,
        FloatConverter.toFloat16Bytes _,
        Seq(
          0f -> 0x0000,
          -0f -> 0x8000,
          halfSubnormalTie -> 0x0000,
          Math.nextUp(halfSubnormalTie) -> 0x0001,
          (3f * halfSubnormalTie) -> 0x0002,
          Math.scalb(1f, -14) -> 0x0400,
          1f -> 0x3c00,
          (1f + Math.scalb(1f, -11)) -> 0x3c00,
          (1f + 3f * Math.scalb(1f, -11)) -> 0x3c02,
          65519f -> 0x7bff,
          -65504f -> 0xfbff
        )
      ),
      (
        VectorElementType.BFloat16,
        FloatConverter.floatToBfloat16Bits _,
        FloatConverter.toBFloat16Bytes _,
        Seq(
          0f -> 0x0000,
          -0f -> 0x8000,
          JavaFloat.intBitsToFloat(0x00010000) -> 0x0001,
          Math.scalb(1f, -126) -> 0x0080,
          1f -> 0x3f80,
          (1f + Math.scalb(1f, -8)) -> 0x3f80,
          (1f + Math.scalb(1f, -7)) -> 0x3f81,
          Float.MaxValue -> 0x7f7f,
          -Float.MaxValue -> 0xff7f
        )
      )
    ).foreach { case (elementType, bitsOf, encode, cases) =>
      cases.foreach { case (value, bits) =>
        bitsOf(value) shouldBe bits
        encode(value) shouldBe Seq(bits.toByte, (bits >>> 8).toByte)
      }
      val queries =
        Seq(cases.map(_._1).toArray, cases.reverse.map(_._1).toArray)
      val expected = (cases ++ cases.reverse).flatMap { case (_, bits) =>
        Seq(bits.toByte, (bits >>> 8).toByte)
      }
      val layout = VectorLayout(elementType, cases.size)
      QueryMatrix.packFloats(queries, layout).toSeq shouldBe expected
      val matrix = QueryMatrix.ofFloats(queries, layout, allocator)
      try {
        matrix.buffer.position() shouldBe 0
        matrix.buffer.limit() shouldBe expected.size
        matrix.buffer.capacity() shouldBe expected.size
        val buffer = read(matrix)
        expected.indices.map(buffer.get) shouldBe expected
      } finally matrix.close()
    }
  }

  test("int8 queries arrive as the bytes they already are") {
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
  }

  test("a binary field takes no queries") {
    val binary = VectorLayout(VectorElementType.Bit, 16)

    val bytes = the[IllegalArgumentException] thrownBy QueryMatrix.ofBytes(
      Seq(Array[Byte](1, 2)),
      binary,
      allocator
    )
    bytes.getMessage should include("int8 fields, not for Bit")
    val floats = the[IllegalArgumentException] thrownBy QueryMatrix
      .packFloats(Seq(Array.fill(16)(0f)), binary)
    floats.getMessage should include("bfloat16 fields, not for Bit")
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
    for {
      elementType <- Seq(
        VectorElementType.Float32,
        VectorElementType.Float16,
        VectorElementType.BFloat16
      )
      value <- Seq(Float.NaN, Float.PositiveInfinity, Float.NegativeInfinity)
    } {
      val failure = the[IllegalArgumentException] thrownBy QueryMatrix.ofFloats(
        Seq(Array(1f, 2f), Array(3f, value)),
        VectorLayout(elementType, 2),
        allocator
      )
      failure.getMessage should include("not finite")
      allocator.getAllocatedMemory shouldBe 0L
    }

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
        built.buffer.position() shouldBe 0
        built.buffer.limit() shouldBe packed.length
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

    Seq(65520f, -65520f, Float.MaxValue, -Float.MaxValue).foreach { value =>
      val half = VectorLayout(VectorElementType.Float16, 2)
      val queries = Seq(Array(1f, 2f), Array(3f, value))
      val packedFailure = the[DataParseException] thrownBy
        QueryMatrix.packFloats(queries, half)
      val builtFailure = the[DataParseException] thrownBy
        QueryMatrix.ofFloats(queries, half, allocator)
      builtFailure.getMessage shouldBe packedFailure.getMessage
      allocator.getAllocatedMemory shouldBe 0L
    }
  }
}
