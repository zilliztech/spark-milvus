package com.zilliz.milvus.storage.write.exec

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.write.exec.ColumnGroupSplit.Column
import io.milvus.grpc.schema.DataType

/** Milvus's column-group rule, as patterns for milvus-storage's writer. */
class ColumnGroupSplitTest extends AnyFunSuite with Matchers {

  private val rowId = Column(0L, DataType.Int64, isKey = false)
  private val timestamp = Column(1L, DataType.Int64, isKey = false)
  private val pk = Column(100L, DataType.Int64, isKey = true)
  private val name = Column(101L, DataType.VarChar, isKey = false)
  private val vector = Column(102L, DataType.FloatVector, isKey = false)
  private val age = Column(103L, DataType.Int32, isKey = false)
  private val doc = Column(104L, DataType.JSON, isKey = false)
  private val text = Column(105L, DataType.Text, isKey = false)
  private val sparse = Column(106L, DataType.SparseFloatVector, isKey = false)

  test(
    "the segment Milvus wrote on UAT: system fields with the pk, the vector alone, the rest together"
  ) {
    ColumnGroupSplit.milvusPatterns(
      Seq(pk, rowId, timestamp, name, vector)
    ) shouldBe
      Seq("^(100|0|1)$", "^102$")
  }

  test(
    "a connector-written segment has no system fields: the pk alone is the first group"
  ) {
    ColumnGroupSplit.milvusPatterns(Seq(pk, name, vector)) shouldBe Seq(
      "^(100)$",
      "^102$"
    )
  }

  test(
    "every vector and Text field gets its own group; the other scalars share the last"
  ) {
    ColumnGroupSplit.milvusPatterns(
      Seq(pk, name, vector, age, doc, text, sparse)
    ) shouldBe
      Seq("^(100)$", "^102$", "^105$", "^106$")
  }

  test("a field averaging 1024 bytes or more per value gets its own group") {
    val avg: Long => Option[Long] = {
      case 104L => Some(1024L)
      case 101L => Some(1023L)
      case _    => None
    }
    ColumnGroupSplit.milvusPatterns(Seq(pk, name, doc, age), avg) shouldBe
      Seq("^(100)$", "^104$")
  }

  test("a partition key or clustering key joins the first group") {
    val partitionKey = Column(107L, DataType.VarChar, isKey = true)
    ColumnGroupSplit.milvusPatterns(Seq(pk, partitionKey, name)) shouldBe Seq(
      "^(100|107)$"
    )
  }

  test(
    "columns written without any key field, as backfill does, get only the per-field groups"
  ) {
    ColumnGroupSplit.milvusPatterns(Seq(vector, age)) shouldBe Seq("^102$")
    ColumnGroupSplit.milvusPatterns(Seq(age)) shouldBe Seq.empty
  }

  test(
    "the writer properties name the schema_based policy, or nothing for no patterns"
  ) {
    ColumnGroupSplit.writerProperties(Seq.empty) shouldBe Map.empty
    ColumnGroupSplit.writerProperties(Seq("^(100)$", "^102$")) shouldBe Map(
      "writer.policy" -> "schema_based",
      "writer.split.schema_based.patterns" -> "^(100)$,^102$"
    )
    an[IllegalArgumentException] should be thrownBy
      ColumnGroupSplit.writerProperties(Seq("^(100,101)$"))
  }
}
