package com.zilliz.spark.connector.write

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.spark.connector.catalog.MilvusHybridTimestamp

/** The two system columns a connector-written segment carries (decision 22,
  * docs/design/architecture/write.html section 6).
  */
class SystemFieldsWriteTest extends AnyFunSuite with Matchers {

  private val schema = StructType(
    Seq(
      StructField("id", LongType, nullable = false),
      StructField("score", LongType, nullable = true)
    )
  )

  private val fieldIds = Map("id" -> 100L, "score" -> 101L)

  test("a new segment carries RowID and Timestamp after its own columns") {
    val arrow = MilvusV3Writer.arrowSchemaFor(schema, fieldIds, true)

    arrow.getFields.asScala.map(_.getName).toSeq shouldBe
      Seq("100", "101", "0", "1")
    arrow.getFields.asScala.takeRight(2).foreach(_.isNullable shouldBe false)
  }

  test("a backfill adds columns to a segment that already has them") {
    val arrow = MilvusV3Writer.arrowSchemaFor(schema, fieldIds, false)

    arrow.getFields.asScala.map(_.getName).toSeq shouldBe Seq("100", "101")
  }

  test("a write that carries the system columns itself is not given more") {
    val declared = StructType(
      schema.fields :+ StructField("Timestamp", LongType, nullable = false)
    )

    the[IllegalArgumentException] thrownBy MilvusV3Writer.arrowSchemaFor(
      declared,
      fieldIds + ("Timestamp" -> 1L),
      true
    )
  }

  test("the job's time becomes the first HybridTS of its millisecond") {
    // Milvus packs a timestamp as physical milliseconds shifted by 18 bits.
    MilvusHybridTimestamp.ofMillis(0L) shouldBe 0L
    MilvusHybridTimestamp.ofMillis(1L) shouldBe (1L << 18)
    MilvusHybridTimestamp.ofMillis(1789708541743L) shouldBe
      (1789708541743L << 18)
    MilvusHybridTimestamp.ofMillis(-1L) shouldBe 0L
    MilvusHybridTimestamp.ofMillis(Long.MaxValue) shouldBe Long.MaxValue
  }
}
