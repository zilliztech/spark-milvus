package com.zilliz.spark.connector.read

import org.apache.spark.sql.types.{LongType, StructField, StructType}
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

  test("system aliases are omitted when user fields use canonical names") {
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

    val mappings = MilvusPackedV2PartitionReader.buildFieldMappings(schema)

    assert(mappings.fieldNameToId("RowID") == 100L)
    assert(mappings.fieldNameToId("Timestamp") == 101L)
    assert(mappings.fieldNameToId("rowid") == 102L)
    assert(mappings.fieldNameToId("row_id") == 0L)
    assert(mappings.fieldNameToId("timestamp") == 1L)
    assert(!mappings.fieldNameToArrowColumn.contains("RowID"))
    assert(!mappings.fieldNameToArrowColumn.contains("Timestamp"))
    assert(!mappings.fieldNameToArrowColumn.contains("rowid"))
    assert(mappings.fieldNameToArrowColumn("row_id") == "RowID")
    assert(mappings.fieldNameToArrowColumn("timestamp") == "Timestamp")
  }

  test("resolveNeededColumns uses exact user names before system aliases") {
    val schema = CollectionSchema(
      fields = Seq(
        FieldSchema(name = "RowID", fieldID = 100, dataType = DataType.Int64)
      )
    )
    val mappings = MilvusPackedV2PartitionReader.buildFieldMappings(schema)
    val sourceSchema = StructType(Seq(StructField("RowID", LongType)))
    val columnGroups = Seq(
      V2ColumnGroup(
        fieldIds = Seq(100L),
        filePaths = Seq("s3a://bucket/files/insert_log/1/2/3/100/1"),
        fileRowCounts = Seq(1L)
      )
    )

    val columns = MilvusPackedV2PartitionReader.resolveNeededColumns(
      sourceSchema,
      columnGroups,
      mappings,
      neededColumnFieldIds = Seq.empty
    )

    assert(columns.toSeq == Seq("RowID"))
  }

  test(
    "resolveNeededColumns fails when requested field is absent from V2 column groups"
  ) {
    val schema = CollectionSchema(
      fields = Seq(
        FieldSchema(name = "pk", fieldID = 100, dataType = DataType.Int64),
        FieldSchema(name = "value", fieldID = 101, dataType = DataType.Int64)
      )
    )
    val mappings = MilvusPackedV2PartitionReader.buildFieldMappings(schema)
    val sourceSchema = StructType(
      Seq(
        StructField("pk", LongType),
        StructField("value", LongType)
      )
    )
    val columnGroups = Seq(
      V2ColumnGroup(
        fieldIds = Seq(100L),
        filePaths = Seq("s3a://bucket/files/insert_log/1/2/3/100/1"),
        fileRowCounts = Seq(1L)
      )
    )

    val err = intercept[IllegalArgumentException] {
      MilvusPackedV2PartitionReader.resolveNeededColumns(
        sourceSchema,
        columnGroups,
        mappings,
        neededColumnFieldIds = Seq(100L, 101L)
      )
    }

    assert(err.getMessage.contains("do not contain requested columns: value"))
  }
}
