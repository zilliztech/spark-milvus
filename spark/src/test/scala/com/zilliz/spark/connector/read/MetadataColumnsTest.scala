package com.zilliz.spark.connector.read

import org.apache.spark.sql.types.{LongType, MetadataBuilder, StructField}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.spark.connector.types.RowIdentity

class MetadataColumnsTest extends AnyFunSuite with Matchers {

  test("a stored field named _segment_id is bound to its storage column") {
    val field = StructField(
      MetadataColumns.SegmentId,
      LongType,
      nullable = false,
      metadata = new MetadataBuilder()
        .putLong(FieldMetadata.MilvusFieldIdMetadataKey, 100L)
        .build()
    )
    MetadataColumns.isSynthetic(field) shouldBe false
    MetadataColumns.isSynthetic(MetadataColumns.partitionIdField) shouldBe true
  }

  test("row identity carries the planned snapshot and physical position") {
    val column = MetadataColumns
      .columnFor(RowIdentity.ColumnName, 20L, 30L, 5L, snapshotId = 7L)
      .get
    try {
      val identity = column.getStruct(2)
      (identity.getLong(0), identity.getLong(1), identity.getLong(2)) shouldBe
        ((7L, 30L, 7L))
    } finally column.close()

    intercept[IllegalStateException] {
      MetadataColumns.columnFor(RowIdentity.ColumnName, 20L, 30L, 5L)
    }
  }
}
