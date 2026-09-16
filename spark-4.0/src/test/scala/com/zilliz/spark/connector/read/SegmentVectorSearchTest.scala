package com.zilliz.spark.connector.read

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  FloatType,
  MetadataBuilder,
  StringType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.codec.FloatConverter
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.spark.connector.options.VectorSearch
import com.zilliz.spark.connector.types.ArrowConverter
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class SegmentVectorSearchTest extends AnyFunSuite {
  private def binaryField(dataType: DataType): StructField =
    StructField(
      "vector",
      BinaryType,
      nullable = true,
      metadata = new MetadataBuilder()
        .putLong(
          FieldMetadata.MilvusDataTypeMetadataKey,
          dataType.value.toLong
        )
        .build()
    )

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

  test("validateVectorSearchField accepts dense float schemas and metrics") {
    val fields = Seq(
      StructField("vector", ArrayType(FloatType, containsNull = false)),
      binaryField(DataType.FloatVector),
      binaryField(DataType.Float16Vector),
      binaryField(DataType.BFloat16Vector)
    )
    for {
      field <- fields
      metric <- Seq("L2", "IP", "COSINE")
    } SegmentVectorSearch.validateVectorSearchField(field, metric)
  }

  test("validateVectorSearchField rejects unsupported schemas and metrics") {
    val fields = Seq(
      StructField("vector", StringType),
      StructField("vector", BinaryType),
      binaryField(DataType.Int8Vector)
    )
    fields.foreach { field =>
      intercept[IllegalArgumentException] {
        SegmentVectorSearch.validateVectorSearchField(field, "L2")
      }
    }
    val error = intercept[IllegalArgumentException] {
      SegmentVectorSearch.validateVectorSearchField(
        StructField("vector", ArrayType(FloatType)),
        "HAMMING"
      )
    }
    assert(error.getMessage.contains("HAMMING"))
  }

  test("run rejects invalid vector schemas before consuming Arrow batches") {
    val batches = new Iterator[VectorSchemaRoot] {
      override def hasNext: Boolean = fail("must validate before reading")
      override def next(): VectorSchemaRoot =
        fail("must validate before reading")
    }
    val error = intercept[IllegalArgumentException] {
      SegmentVectorSearch.run(
        VectorSearch(Array(1.0f), 1, "L2", "vector"),
        StructType(Seq(StructField("vector", StringType))),
        Map.empty,
        batches,
        (_, _) => false
      )
    }
    assert(error.getMessage.contains("vector"))
    assert(error.getMessage.contains("unsupported vector type"))
  }

  test("extractVector preserves null vectors for exclusion") {
    val row = new GenericInternalRow(Array[Any](null))
    val fields = Seq(
      StructField("vector", ArrayType(FloatType)),
      binaryField(DataType.FloatVector),
      binaryField(DataType.Float16Vector),
      binaryField(DataType.BFloat16Vector)
    )
    fields.foreach { field =>
      assert(SegmentVectorSearch.extractVector(row, 0, field) == null)
    }
  }

  test("run rejects a query dimension different from vector field metadata") {
    val field = StructField(
      "vector",
      ArrayType(FloatType),
      metadata = new MetadataBuilder()
        .putLong(FieldMetadata.MilvusVectorDimensionMetadataKey, 3L)
        .build()
    )
    val error = intercept[IllegalArgumentException] {
      SegmentVectorSearch.run(
        VectorSearch(Array(1.0f, 2.0f), 1, "L2", "vector"),
        StructType(Seq(field)),
        Map.empty,
        Iterator.empty,
        (_, _) => false
      )
    }
    assert(error.getMessage.contains("vector"))
    assert(error.getMessage.contains("dimension 3"))
  }

  test("extractVector reads float arrays without changing their values") {
    val values = Array(1.5f, -2.0f, 0.0f)
    val row = new GenericInternalRow(Array[Any](new GenericArrayData(values)))
    assert(
      SegmentVectorSearch
        .extractVector(row, 0, StructField("vector", ArrayType(FloatType)))
        .sameElements(values)
    )
  }

  test("extractVector rejects null array elements even in a non-null schema") {
    val row = new GenericInternalRow(
      Array[Any](new GenericArrayData(Array[Any](1.0f, null)))
    )
    val error = intercept[IllegalArgumentException] {
      SegmentVectorSearch.extractVector(
        row,
        0,
        StructField("vector", ArrayType(FloatType, containsNull = false))
      )
    }
    assert(error.getMessage.contains("vector"))
    assert(error.getMessage.contains("null element at index 1"))
  }

  test("decodeBinaryTypeVectorForSearch decodes little-endian FloatVector") {
    val bytes = ByteBuffer
      .allocate(8)
      .order(ByteOrder.LITTLE_ENDIAN)
      .putFloat(1.5f)
      .putFloat(-2.0f)
      .array()
    assert(
      SegmentVectorSearch
        .decodeBinaryTypeVectorForSearch(
          bytes,
          binaryField(DataType.FloatVector)
        )
        .sameElements(Array(1.5f, -2.0f))
    )
  }

  test(
    "decodeBinaryTypeVectorForSearch rejects partial floating-point values"
  ) {
    Seq(
      DataType.FloatVector -> 5,
      DataType.Float16Vector -> 3,
      DataType.BFloat16Vector -> 3
    ).foreach { case (dataType, size) =>
      val error = intercept[IllegalArgumentException] {
        SegmentVectorSearch.decodeBinaryTypeVectorForSearch(
          new Array[Byte](size),
          binaryField(dataType)
        )
      }
      assert(error.getMessage.contains(s"$size bytes"))
      assert(error.getMessage.contains("multiple of"))
    }
  }

  test("binary decoding distinguishes null from an empty vector") {
    val field = binaryField(DataType.FloatVector)
    assert(
      SegmentVectorSearch.decodeBinaryTypeVectorForSearch(null, field) == null
    )
    assert(
      SegmentVectorSearch
        .decodeBinaryTypeVectorForSearch(Array.emptyByteArray, field)
        .isEmpty
    )
  }
}
