package com.zilliz.spark.connector.search

import java.nio.{ByteBuffer, ByteOrder}

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

  /** Query ids are typed; a null vector stands for a NULL value. */
  private def pack(
      queries: Seq[(Long, Array[Float])],
      metric: MetricType = MetricType.L2
  ): (Array[Long], Array[Byte]) =
    SearchQueries.packPartition(
      queries.iterator,
      float32,
      metric
    )

  test("queries are packed in the order they arrive, ids alongside") {
    val (ids, vectors) =
      pack(Seq(7L -> Array(1f, 2f), 3L -> Array(3f, 4f)))

    ids shouldBe Array(7L, 3L)
    floats(vectors) shouldBe Seq(1f, 2f, 3f, 4f)
  }

  test("a query of another dimension names its query id") {
    val failure = the[IllegalArgumentException] thrownBy pack(
      Seq(11L -> Array(1f, 2f), 12L -> Array(1f))
    )

    failure.getMessage should include("Query 12 has 1 values")
  }

  test("a query with a value that is not finite is refused") {
    val failure = the[IllegalArgumentException] thrownBy pack(
      Seq(5L -> Array(1f, Float.NaN))
    )

    failure.getMessage should include("not finite")
  }

  test("a zero query has no COSINE answer, and an L2 one is fine") {
    val zero = Seq(5L -> Array(0f, 0f))
    val failure =
      the[IllegalArgumentException] thrownBy pack(zero, MetricType.Cosine)

    failure.getMessage should include("zero norm")
    pack(zero, MetricType.L2)._1 shouldBe Array(5L)
  }

  test("a query without a vector is refused") {
    val noVector = the[IllegalArgumentException] thrownBy
      pack(Seq(1L -> (null: Array[Float])))
    noVector.getMessage should include("Query 1 has no vector")
  }

  test("the bytes of a query set are its queries times its row") {
    SearchQueries.bytes(1000L, float32) shouldBe 8000L
    SearchQueries.bytes(
      1000L,
      VectorLayout(VectorElementType.Float16, 16)
    ) shouldBe 32000L
  }
}
