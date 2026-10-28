package com.zilliz.spark.connector.search

import java.nio.{ByteBuffer, ByteOrder}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.milvus.storage.DataParseException

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
  ): (Array[Long], Array[Byte]) = {
    val chunks = SearchQueries
      .packedChunks(queries.iterator, float32, metric)
      .toVector
    (chunks.flatMap(_._1).toArray, chunks.flatMap(_._2).toArray)
  }

  test("packing reads at most one chunk ahead and emits no empty tail") {
    var read = 0
    val rows = (0 until 4096).iterator.map { index =>
      read += 1
      index.toLong -> Array(index.toFloat, -index.toFloat)
    }
    val chunks = SearchQueries.packedChunks(rows, float32, MetricType.L2)
    read shouldBe 0
    chunks.next()._1 shouldBe (0L until 2048L).toArray
    read shouldBe 2048
    chunks.next()._1 shouldBe (2048L until 4096L).toArray
    read shouldBe 4096
    chunks.hasNext shouldBe false
    SearchQueries
      .packedChunks(Iterator.empty, float32, MetricType.L2)
      .hasNext shouldBe false
  }

  test("invalid rows fail before reading later input") {
    val rows = Iterator(12L -> (null: Array[Float])) ++
      Iterator.continually(throw new IllegalStateException("read too far"))
    val chunks = SearchQueries.packedChunks(rows, float32, MetricType.L2)
    val failure = the[IllegalArgumentException] thrownBy chunks.next()
    failure.getMessage should include("Query 12 has no vector")
  }

  test("encoding overflow fails after previously packed chunks") {
    val rows = Iterator.fill(2048)(1L -> Array(1f, 2f)) ++
      Iterator(2L -> Array(65520f, 0f))
    val chunks = SearchQueries.packedChunks(
      rows,
      VectorLayout(VectorElementType.Float16, 2),
      MetricType.L2
    )
    chunks.next()._1.length shouldBe 2048
    val failure = the[DataParseException] thrownBy chunks.next()
    failure.getMessage should include("float16 range")
  }

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
