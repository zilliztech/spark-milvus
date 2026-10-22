package com.zilliz.spark.connector.types

import org.apache.arrow.vector.{
  FieldVector,
  FixedSizeBinaryVector,
  VarBinaryVector
}
import org.apache.spark.sql.types.{DataType, FloatType, LongType, StructField}

import com.zilliz.milvus.storage.schema.FieldMetadata
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Type metadata and layout checks shared by Arrow readers and writers. */
object ArrowValueTypes {
  def milvusDataType(field: StructField): Option[MilvusDataType] = {
    Option(field.metadata)
      .filter(_.contains(FieldMetadata.MilvusDataTypeMetadataKey))
      .map(_.getLong(FieldMetadata.MilvusDataTypeMetadataKey).toInt)
      .map(MilvusDataType.fromValue)
  }

  def isBinaryBackedVector(vector: FieldVector): Boolean =
    vector.isInstanceOf[FixedSizeBinaryVector] ||
      vector.isInstanceOf[VarBinaryVector]

  def requireSparseSparkTypes(
      keyType: DataType,
      valueType: DataType
  ): Unit = {
    if (keyType != LongType || valueType != FloatType) {
      throw new IllegalArgumentException(
        s"SparseFloatVector requires MapType(LongType, FloatType), got MapType($keyType, $valueType)"
      )
    }
  }
}
