package com.zilliz.spark.connector.read

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.spark.connector.options.MilvusOption

class MetadataColumnsTest extends AnyFunSuite with Matchers {

  test("row wrapping synthesizes only metadata columns explicitly requested") {
    val underlying = new RowOffsetReader {
      private var available = true

      override def next(): Boolean = {
        val result = available
        available = false
        result
      }

      override def get(): InternalRow =
        new GenericInternalRow(Array[Any](77L))

      override def lastReturnedRowOffset: Long = 5L

      override def close(): Unit = ()
    }
    val schema = StructType(
      Seq(
        StructField(
          MilvusOption.MilvusExtraColumnSegmentID,
          LongType,
          nullable = false
        ),
        StructField(
          MilvusOption.MilvusExtraColumnRowOffset,
          LongType,
          nullable = false
        )
      )
    )
    val reader = MetadataColumns.wrapRows(
      underlying,
      schema,
      requested = Set(MilvusOption.MilvusExtraColumnRowOffset),
      partitionName = "20",
      segmentId = 30L
    )

    reader.next() shouldBe true
    val row = reader.get()
    row.getLong(0) shouldBe 77L
    row.getLong(1) shouldBe 5L
  }
}
