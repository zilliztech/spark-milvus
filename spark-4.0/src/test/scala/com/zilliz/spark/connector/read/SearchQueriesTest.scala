package com.zilliz.spark.connector.read

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}

/** The query set a search takes, checked and packed before anything is read. */
class SearchQueriesTest extends AnyFunSuite with Matchers {

  private val float32 = VectorLayout(VectorElementType.Float32, 2)

  private def schema(vector: DataType): StructType = StructType(
    Seq(
      StructField(SearchQueries.IdColumn, LongType),
      StructField(SearchQueries.VectorColumn, vector)
    )
  )

  private def floats(packed: Array[Byte]): Seq[Float] = {
    val buffer = ByteBuffer.wrap(packed).order(ByteOrder.nativeOrder())
    (0 until packed.length / 4).map(index => buffer.getFloat(index * 4))
  }

  test("a query set carries query_id and vector in the field's type") {
    SearchQueries.check(schema(ArrayType(FloatType)), float32)
    SearchQueries.check(
      schema(ArrayType(ShortType)),
      VectorLayout(VectorElementType.Int8, 2)
    )
    SearchQueries.check(
      schema(BinaryType),
      VectorLayout(VectorElementType.Bit, 16)
    )
  }

  test("a query set of the wrong element type is refused") {
    the[IllegalArgumentException] thrownBy SearchQueries.check(
      schema(ArrayType(DoubleType)),
      float32
    )
    the[IllegalArgumentException] thrownBy SearchQueries.check(
      schema(ArrayType(FloatType)),
      VectorLayout(VectorElementType.Int8, 2)
    )
    the[IllegalArgumentException] thrownBy SearchQueries.check(
      schema(BinaryType),
      float32
    )
  }

  test("a query set without the two columns names what it has") {
    val failure = the[IllegalArgumentException] thrownBy SearchQueries.check(
      StructType(Seq(StructField("id", LongType))),
      float32
    )

    failure.getMessage should include("'query_id'")
    failure.getMessage should include("id")
  }

  test("a query id column of another type is refused") {
    val failure = the[IllegalArgumentException] thrownBy SearchQueries.check(
      StructType(
        Seq(
          StructField(SearchQueries.IdColumn, IntegerType),
          StructField(SearchQueries.VectorColumn, ArrayType(FloatType))
        )
      ),
      float32
    )

    failure.getMessage should include("BIGINT")
  }

  test("queries are packed in the order they arrive, ids alongside") {
    val (ids, vectors) = SearchQueries.pack(
      Seq(Row(7L, Seq(1f, 2f)), Row(3L, Seq(3f, 4f))),
      float32,
      "L2"
    )

    ids shouldBe Array(7L, 3L)
    floats(vectors) shouldBe Seq(1f, 2f, 3f, 4f)
  }

  test("int8 queries arrive as shorts and are packed as bytes") {
    val layout = VectorLayout(VectorElementType.Int8, 3)

    val (ids, vectors) = SearchQueries.pack(
      Seq(Row(1L, Seq[Short](1, -2, 3))),
      layout,
      "L2"
    )

    ids shouldBe Array(1L)
    vectors shouldBe Array[Byte](1, -2, 3)
  }

  test("an int8 query outside the byte range is refused") {
    val failure = the[IllegalArgumentException] thrownBy SearchQueries.pack(
      Seq(Row(1L, Seq[Short](1, 300, 3))),
      VectorLayout(VectorElementType.Int8, 3),
      "L2"
    )

    failure.getMessage should include("300")
  }

  test("a query of another dimension names its query id") {
    val failure = the[IllegalArgumentException] thrownBy SearchQueries.pack(
      Seq(Row(11L, Seq(1f, 2f)), Row(12L, Seq(1f))),
      float32,
      "L2"
    )

    failure.getMessage should include("Query 12 has 1 values")
  }

  test("a query with a value that is not finite is refused") {
    val failure = the[IllegalArgumentException] thrownBy SearchQueries.pack(
      Seq(Row(5L, Seq(1f, Float.NaN))),
      float32,
      "L2"
    )

    failure.getMessage should include("not finite")
  }

  test("a zero query has no COSINE answer, and an L2 one is fine") {
    val failure = the[IllegalArgumentException] thrownBy SearchQueries.pack(
      Seq(Row(5L, Seq(0f, 0f))),
      float32,
      "COSINE"
    )

    failure.getMessage should include("zero norm")
    SearchQueries.pack(Seq(Row(5L, Seq(0f, 0f))), float32, "L2")._1 shouldBe
      Array(5L)
  }

  test("a query without an id or a vector is refused") {
    the[IllegalArgumentException] thrownBy SearchQueries.pack(
      Seq(Row(null, Seq(1f, 2f))),
      float32,
      "L2"
    )
    the[IllegalArgumentException] thrownBy SearchQueries.pack(
      Seq(Row(1L, null)),
      float32,
      "L2"
    )
  }

  test("a repeated query id is refused") {
    SearchQueries.checkUnique(Array(4L, 9L, 1L))

    val failure = the[IllegalArgumentException] thrownBy SearchQueries
      .checkUnique(Array(4L, 9L, 4L))

    failure.getMessage should include("Query id 4")
  }

  test("the bytes of a query set are its queries times its row") {
    SearchQueries.bytes(1000L, float32) shouldBe 8000L
    SearchQueries.bytes(
      1000L,
      VectorLayout(VectorElementType.Bit, 128)
    ) shouldBe 16000L
  }
}
