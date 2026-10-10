package com.zilliz.spark.connector.read

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.Row
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}

/** The query set a search takes, packed before anything is read. */
class SearchQueriesTest extends AnyFunSuite with Matchers {

  private val float32 = VectorLayout(VectorElementType.Float32, 2)

  private def floats(packed: Array[Byte]): Seq[Float] = {
    val buffer = ByteBuffer.wrap(packed).order(ByteOrder.nativeOrder())
    (0 until packed.length / 4).map(index => buffer.getFloat(index * 4))
  }

  test("queries are packed in the order they arrive, ids alongside") {
    val (ids, vectors) = SearchQueries.pack(
      Seq(Row(7L, Seq(1f, 2f)), Row(3L, Seq(3f, 4f))),
      float32,
      MetricType.L2
    )

    ids shouldBe Array(7L, 3L)
    floats(vectors) shouldBe Seq(1f, 2f, 3f, 4f)
  }

  test("a query of another dimension names its query id") {
    val failure = the[IllegalArgumentException] thrownBy SearchQueries.pack(
      Seq(Row(11L, Seq(1f, 2f)), Row(12L, Seq(1f))),
      float32,
      MetricType.L2
    )

    failure.getMessage should include("Query 12 has 1 values")
  }

  test("a query with a value that is not finite is refused") {
    val failure = the[IllegalArgumentException] thrownBy SearchQueries.pack(
      Seq(Row(5L, Seq(1f, Float.NaN))),
      float32,
      MetricType.L2
    )

    failure.getMessage should include("not finite")
  }

  test("a zero query has no COSINE answer, and an L2 one is fine") {
    val failure = the[IllegalArgumentException] thrownBy SearchQueries.pack(
      Seq(Row(5L, Seq(0f, 0f))),
      float32,
      MetricType.Cosine
    )

    failure.getMessage should include("zero norm")
    SearchQueries
      .pack(Seq(Row(5L, Seq(0f, 0f))), float32, MetricType.L2)
      ._1 shouldBe
      Array(5L)
  }

  test("a query without an id or a vector is refused") {
    the[IllegalArgumentException] thrownBy SearchQueries.pack(
      Seq(Row(null, Seq(1f, 2f))),
      float32,
      MetricType.L2
    )
    the[IllegalArgumentException] thrownBy SearchQueries.pack(
      Seq(Row(1L, null)),
      float32,
      MetricType.L2
    )
  }

  test("the bytes of a query set are its queries times its row") {
    SearchQueries.bytes(1000L, float32) shouldBe 8000L
    SearchQueries.bytes(
      1000L,
      VectorLayout(VectorElementType.Float16, 16)
    ) shouldBe 32000L
  }
}
