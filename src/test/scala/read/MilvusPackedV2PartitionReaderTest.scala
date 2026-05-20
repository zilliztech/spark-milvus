package com.zilliz.spark.connector.read

import org.scalatest.funsuite.AnyFunSuite

import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class MilvusPackedV2PartitionReaderTest extends AnyFunSuite {
  test(
    "lowercase system aliases are omitted when user fields use those names"
  ) {
    val schema = CollectionSchema(
      fields = Seq(
        FieldSchema(name = "row_id", fieldID = 100, dataType = DataType.Int64),
        FieldSchema(
          name = "timestamp",
          fieldID = 101,
          dataType = DataType.Int64
        )
      )
    )

    val mappings = MilvusPackedV2PartitionReader.buildFieldMappings(schema)

    assert(mappings.fieldNameToId("row_id") == 100L)
    assert(mappings.fieldNameToId("timestamp") == 101L)
    assert(!mappings.fieldNameToArrowColumn.contains("row_id"))
    assert(!mappings.fieldNameToArrowColumn.contains("timestamp"))
    assert(mappings.fieldNameToArrowColumn("RowID") == "RowID")
    assert(mappings.fieldNameToArrowColumn("Timestamp") == "Timestamp")
  }
}
