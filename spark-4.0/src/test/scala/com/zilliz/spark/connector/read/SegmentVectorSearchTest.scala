package com.zilliz.spark.connector.read

import org.apache.spark.sql.types.{
  BinaryType,
  MetadataBuilder,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.spark.connector.types.ArrowConverter
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class SegmentVectorSearchTest extends AnyFunSuite {
  test("validateVectorSearchField rejects BinaryVector dense search") {
    val field = StructField(
      "binary_vec",
      BinaryType,
      nullable = true,
      metadata = new MetadataBuilder()
        .putLong(
          FieldMetadata.MilvusDataTypeMetadataKey,
          DataType.BinaryVector.value.toLong
        )
        .build()
    )

    val err = intercept[IllegalArgumentException] {
      SegmentVectorSearch.validateVectorSearchField(field, "L2")
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
          FieldMetadata.MilvusDataTypeMetadataKey,
          DataType.BinaryVector.value.toLong
        )
        .build()
    )

    val err = intercept[IllegalArgumentException] {
      SegmentVectorSearch.decodeBinaryTypeVectorForSearch(
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
          FieldMetadata.MilvusDataTypeMetadataKey,
          DataType.Float16Vector.value.toLong
        )
        .build()
    )
    val bytes = FloatConverter.toFloat16Bytes(1.5f).toArray ++
      FloatConverter.toFloat16Bytes(-2.0f).toArray

    val decoded =
      SegmentVectorSearch.decodeBinaryTypeVectorForSearch(bytes, field)

    assert(decoded.sameElements(Array(1.5f, -2.0f)))
  }

  test("decodeBinaryTypeVectorForSearch decodes BFloat16Vector bytes") {
    val field = StructField(
      "bf16_vec",
      BinaryType,
      nullable = true,
      metadata = new MetadataBuilder()
        .putLong(
          FieldMetadata.MilvusDataTypeMetadataKey,
          DataType.BFloat16Vector.value.toLong
        )
        .build()
    )
    val bytes = FloatConverter.toBFloat16Bytes(1.5f).toArray ++
      FloatConverter.toBFloat16Bytes(-2.0f).toArray

    val decoded =
      SegmentVectorSearch.decodeBinaryTypeVectorForSearch(bytes, field)

    assert(decoded.sameElements(Array(1.5f, -2.0f)))
  }
}
