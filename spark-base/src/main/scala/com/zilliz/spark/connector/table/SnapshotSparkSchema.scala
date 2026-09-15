package com.zilliz.spark.connector.table

import org.apache.spark.sql.types._

import com.zilliz.milvus.storage.schema.{FieldMetadata, MilvusTypes}
import com.zilliz.milvus.storage.snapshot.json.{CollectionSchemaJson, FieldJson}
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** The `CollectionSchemaJson` inside a snapshot to a Spark StructType.
  *
  * Parsing the snapshot JSON is layer 2 (`core.snapshot.json`); the Spark types
  * are layer 3, so the mapping sits here.
  */
object SnapshotSparkSchema {

  /** Convert snapshot CollectionSchemaJson to Spark StructType
    *
    * @param schema
    *   CollectionSchemaJson from snapshot metadata
    * @param includeSystemFields
    *   Whether to include RowID and Timestamp system fields
    * @return
    *   Spark StructType representing the collection schema
    */
  def toSparkSchema(
      schema: CollectionSchemaJson,
      includeSystemFields: Boolean = false
  ): StructType = {
    val userFields = schema.fields
      .filterNot(f =>
        !includeSystemFields && (f.name == "RowID" || f.name == "Timestamp")
      )
      .map(fieldToStructField)
    StructType(userFields)
  }

  /** Convert a FieldJson to Spark StructField with Milvus metadata preserved.
    */
  def fieldToStructField(field: FieldJson): StructField = {
    val metadata = new MetadataBuilder()
      .putLong(FieldMetadata.MilvusDataTypeMetadataKey, field.dataType)
    val milvusType = MilvusDataType.fromValue(field.dataType)
    if (MilvusTypes.isDenseVectorType(milvusType)) {
      field.getTypeParam("dim").foreach { rawDimension =>
        metadata.putLong(
          FieldMetadata.MilvusVectorDimensionMetadataKey,
          MilvusTypes.parseVectorDimension(field.name, rawDimension)
        )
      }
    }

    StructField(
      field.name,
      dataTypeToSparkType(field.dataType, field.elementType),
      nullable = field.nullable.getOrElse(true),
      metadata = metadata.build()
    )
  }

  /** Convert a FieldJson to Spark DataType
    *
    * @param field
    *   FieldJson from snapshot schema
    * @return
    *   Corresponding Spark DataType
    */
  def fieldToSparkType(field: FieldJson): DataType = {
    fieldToStructField(field).dataType
  }

  /** Convert Milvus data type to Spark DataType
    *
    * @param dataType
    *   Milvus data type integer code
    * @param typeParams
    *   Optional type parameters (e.g., dim for vectors, max_length for varchar)
    * @return
    *   Corresponding Spark DataType
    */
  private def dataTypeToSparkType(
      dataType: Int,
      elementType: Int
  ): DataType = {
    dataType match {
      case 1   => BooleanType // Bool
      case 2   => ByteType // Int8
      case 3   => ShortType // Int16
      case 4   => IntegerType // Int32
      case 5   => LongType // Int64
      case 10  => FloatType // Float
      case 11  => DoubleType // Double
      case 20  => StringType // String
      case 21  => StringType // VarChar
      case 22  => ArrayType(dataTypeToSparkType(elementType, 0)) // Array
      case 23  => StringType // JSON (as string)
      case 24  => StringType // Geometry
      case 25  => StringType // Text
      case 26  => LongType // Timestamptz
      case 100 => BinaryType // BinaryVector
      case 101 => ArrayType(FloatType) // FloatVector
      case 102 => ArrayType(FloatType) // Float16Vector
      case 103 => ArrayType(FloatType) // BFloat16Vector
      case 104 => MapType(LongType, FloatType) // SparseFloatVector
      case 105 => ArrayType(ShortType) // Int8Vector
      case _   => BinaryType // Unknown types as binary
    }
  }
}
