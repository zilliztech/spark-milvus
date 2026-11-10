package com.zilliz.spark.connector.types

import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RowIdColumnTest extends AnyFunSuite with Matchers {

  test(
    "a batch's row identities share the snapshot and segment and count offsets"
  ) {
    val column = new RowIdColumn(7L, 30L, 100L)
    column.dataType() shouldBe RowIdentity.dataType
    val first = column.getStruct(0)
    first.getLong(RowIdentity.SnapshotIdOrdinal) shouldBe 7L
    first.getLong(RowIdentity.SegmentIdOrdinal) shouldBe 30L
    first.getLong(RowIdentity.RowOffsetOrdinal) shouldBe 100L
    column.getStruct(3).getLong(RowIdentity.RowOffsetOrdinal) shouldBe 103L
  }

  test("a selection of a struct column selects its fields the same way") {
    // Rows 1 and 3 survive, as when a predicate or a delete excludes the rest.
    val selected = new SelectedRowsColumn(
      new RowIdColumn(7L, 30L, 100L),
      Array(1, 3),
      RowIdentity.dataType
    )
    val batch = new ColumnarBatch(Array[ColumnVector](selected), 2)
    val offsets = (0 until batch.numRows()).map(row =>
      batch.getRow(row).getStruct(0, 3).getLong(RowIdentity.RowOffsetOrdinal)
    )
    offsets shouldBe Seq(101L, 103L)
    selected.getChild(RowIdentity.RowOffsetOrdinal).dataType() shouldBe LongType
  }
}
