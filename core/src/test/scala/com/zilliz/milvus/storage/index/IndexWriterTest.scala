package com.zilliz.milvus.storage.index

import java.nio.ByteBuffer

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}

/** What a build accepts and what it hands Knowhere
  * (docs/design/architecture/vector-search.html section 2.7).
  */
class IndexWriterTest extends AnyFunSuite with Matchers {

  private val layout = VectorLayout(VectorElementType.Float32, 4)

  private def buffer(rows: Int): ByteBuffer =
    ByteBuffer.allocateDirect(rows * layout.rowBytes)

  test("the build parameters carry the metric, the dimension and the tuning") {
    IndexWriter.buildParameters(
      "COSINE",
      layout,
      Map("M" -> "16", "efConstruction" -> "200")
    ) shouldBe """{"metric_type":"COSINE","dim":4,"M":16,"efConstruction":200}"""

    IndexWriter.buildParameters(
      "L2",
      layout,
      Map("sq_type" -> "SQ4U", "refine" -> "true", "refine_ratio" -> "1.5")
    ) shouldBe
      """{"metric_type":"L2","dim":4,"refine":true,"refine_ratio":1.5,"sq_type":"SQ4U"}"""
  }

  test("the column and the search decide the metric and the dimension") {
    the[IllegalArgumentException] thrownBy IndexWriter.buildParameters(
      "L2",
      layout,
      Map("metric_type" -> "IP")
    )
    the[IllegalArgumentException] thrownBy IndexWriter.buildParameters(
      "L2",
      layout,
      Map("dim" -> "8")
    )
  }

  test("only the index types this connector can load again are built") {
    val failure = the[IllegalArgumentException] thrownBy IndexWriter.build(
      buffer(4),
      4L,
      layout,
      "DISKANN",
      "L2",
      indexVersion = 8
    )

    failure.getMessage should include("not DISKANN")
  }

  test("a metric has to belong to the element type") {
    the[IllegalArgumentException] thrownBy IndexWriter.build(
      buffer(4),
      4L,
      layout,
      "HNSW",
      "HAMMING",
      indexVersion = 8
    )
    the[IllegalArgumentException] thrownBy IndexWriter.build(
      ByteBuffer.allocateDirect(8),
      4L,
      VectorLayout(VectorElementType.Bit, 16),
      "BIN_IVF_FLAT",
      "COSINE",
      indexVersion = 8
    )
  }

  test("a buffer that cannot hold the rows is refused") {
    val failure = the[IllegalArgumentException] thrownBy IndexWriter.build(
      buffer(2),
      4L,
      layout,
      "HNSW",
      "L2",
      indexVersion = 8
    )

    failure.getMessage should include("32 bytes")
    the[IllegalArgumentException] thrownBy IndexWriter.build(
      buffer(4),
      0L,
      layout,
      "HNSW",
      "L2",
      indexVersion = 8
    )
  }

  test("a build parameter without a name or a value is refused") {
    the[IllegalArgumentException] thrownBy IndexWriter.build(
      buffer(4),
      4L,
      layout,
      "HNSW",
      "L2",
      indexVersion = 8,
      parameters = Map("M" -> "")
    )
  }
}
