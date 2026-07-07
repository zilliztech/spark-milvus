package com.zilliz.spark.connector.read

import org.apache.spark.sql.types.{
  BinaryType,
  MetadataBuilder,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.spark.connector.serde.ArrowConverter
import com.zilliz.spark.connector.FloatConverter
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class MilvusLoonPartitionReaderTest extends AnyFunSuite {
  test("delete filtering uses field-id column name for timestamp") {
    assert(MilvusLoonPartitionReader.TimestampColumnName == "1")
  }

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

  test("validateVectorSearchField rejects BinaryVector dense search") {
    val field = StructField(
      "binary_vec",
      BinaryType,
      nullable = true,
      metadata = new MetadataBuilder()
        .putLong(
          ArrowConverter.MilvusDataTypeMetadataKey,
          DataType.BinaryVector.value.toLong
        )
        .build()
    )

    val err = intercept[IllegalArgumentException] {
      MilvusLoonPartitionReader.validateVectorSearchField(field, "L2")
    }

    assert(err.getMessage.contains("binary_vec"))
    assert(err.getMessage.contains("BinaryVector"))
    assert(err.getMessage.contains("Hamming/Jaccard"))
  }

  test("decodeBinaryTypeVectorForSearch rejects BinaryVector metadata") {
    val field = StructField(
      "binary_vec",
      BinaryType,
      nullable = true,
      metadata = new MetadataBuilder()
        .putLong(
          ArrowConverter.MilvusDataTypeMetadataKey,
          DataType.BinaryVector.value.toLong
        )
        .build()
    )

    val err = intercept[IllegalArgumentException] {
      MilvusLoonPartitionReader.decodeBinaryTypeVectorForSearch(
        Array[Byte](1, 2, 3, 4),
        field
      )
    }

    assert(err.getMessage.contains("BinaryVector"))
    assert(err.getMessage.contains("Hamming/Jaccard"))
  }

  test("decodeBinaryTypeVectorForSearch decodes Float16Vector bytes") {
    val field = StructField(
      "fp16_vec",
      BinaryType,
      nullable = true,
      metadata = new MetadataBuilder()
        .putLong(
          ArrowConverter.MilvusDataTypeMetadataKey,
          DataType.Float16Vector.value.toLong
        )
        .build()
    )
    val bytes = FloatConverter.toFloat16Bytes(1.5f).toArray ++
      FloatConverter.toFloat16Bytes(-2.0f).toArray

    val decoded =
      MilvusLoonPartitionReader.decodeBinaryTypeVectorForSearch(bytes, field)

    assert(decoded.sameElements(Array(1.5f, -2.0f)))
  }

  test("decodeBinaryTypeVectorForSearch decodes BFloat16Vector bytes") {
    val field = StructField(
      "bf16_vec",
      BinaryType,
      nullable = true,
      metadata = new MetadataBuilder()
        .putLong(
          ArrowConverter.MilvusDataTypeMetadataKey,
          DataType.BFloat16Vector.value.toLong
        )
        .build()
    )
    val bytes = FloatConverter.toBFloat16Bytes(1.5f).toArray ++
      FloatConverter.toBFloat16Bytes(-2.0f).toArray

    val decoded =
      MilvusLoonPartitionReader.decodeBinaryTypeVectorForSearch(bytes, field)

    assert(decoded.sameElements(Array(1.5f, -2.0f)))
  }
}
