package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite

import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class MilvusLoonPartitionReaderTest extends AnyFunSuite {
  test("buildFieldNameToId exposes non-conflicting system aliases") {
    val schema = CollectionSchema(
      fields = Seq(
        FieldSchema(name = "pk", fieldID = 100, dataType = DataType.Int64)
      )
    )

    val mapping = MilvusLoonPartitionReader.buildFieldNameToId(schema)

    assert(mapping("RowID") == 0L)
    assert(mapping("row_id") == 0L)
    assert(mapping("rowid") == 0L)
    assert(mapping("Timestamp") == 1L)
    assert(mapping("timestamp") == 1L)
    assert(mapping("pk") == 100L)
  }

  test("buildFieldNameToId preserves user fields that use system alias names") {
    val schema = CollectionSchema(
      fields = Seq(
        FieldSchema(name = "RowID", fieldID = 100, dataType = DataType.Int64),
        FieldSchema(
          name = "Timestamp",
          fieldID = 101,
          dataType = DataType.Int64
        ),
        FieldSchema(name = "rowid", fieldID = 102, dataType = DataType.Int64)
      )
    )

    val mapping = MilvusLoonPartitionReader.buildFieldNameToId(schema)

    assert(mapping("RowID") == 100L)
    assert(mapping("Timestamp") == 101L)
    assert(mapping("rowid") == 102L)
    assert(mapping("row_id") == 0L)
    assert(mapping("timestamp") == 1L)
  }
}
