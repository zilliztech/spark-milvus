package com.zilliz.spark.connector

import io.milvus.grpc.schema.{
  ArrayArray,
  BoolArray,
  BytesArray,
  DataType,
  DoubleArray,
  FieldData,
  FieldSchema,
  FloatArray,
  GeometryArray,
  IntArray,
  JSONArray,
  LongArray,
  ScalarField,
  SparseFloatArray,
  StringArray,
  VectorField
}

/**
 * MilvusSchemaUtil provides utilities for converting between Milvus and Arrow schemas
 */
object MilvusSchemaUtil {
  def getDim(fieldSchema: FieldSchema): Int = {
    for (param <- fieldSchema.typeParams) {
      if (param.key == "dim") {
        return param.value.toInt
      }
    }
    throw new DataParseException(
      s"Field ${fieldSchema.name} has no dim parameter"
    )
  }

  /**
   * Convert Milvus FieldSchema to Arrow Field
   */
  def convertToArrowField(
      field: FieldSchema,
      arrowType: org.apache.arrow.vector.types.pojo.ArrowType
  ): org.apache.arrow.vector.types.pojo.Field = {
    import scala.collection.JavaConverters._

    val metadata = Map(
      "PARQUET:field_id" -> field.fieldID.toString
    ).asJava

    // Create FieldType with metadata included
    val fieldType = new org.apache.arrow.vector.types.pojo.FieldType(
      true, // nullable
      arrowType,
      null, // dictionary encoding
      metadata
    )

    new org.apache.arrow.vector.types.pojo.Field(
      field.name,
      // field.fieldID.toString,
      fieldType,
      null // children - null for simple types
    )
  }

  /**
   * Convert Milvus CollectionSchema to Arrow Schema
   * This function converts a Milvus collection schema to an Arrow schema format.
   * Now uses the serdeMap for consistent type conversion.
   *
   * @param collectionSchema The Milvus collection schema
   * @return Arrow Schema
   */
  def convertToArrowSchema(
      collectionSchema: io.milvus.grpc.schema.CollectionSchema
  ): org.apache.arrow.vector.types.pojo.Schema = {
    import scala.collection.JavaConverters._
    import org.apache.arrow.vector.types.pojo.{Field, FieldType}

    val arrowFields = scala.collection.mutable.ArrayBuffer[Field]()

    // Helper function to append a field
    def appendArrowField(field: FieldSchema): Unit = {
      // Get dimension for vector types
      val dim = field.dataType match {
        case DataType.BinaryVector | DataType.Float16Vector |
             DataType.BFloat16Vector | DataType.Int8Vector |
             DataType.FloatVector | DataType.ArrayOfVector =>
          try {
            getDim(field)
          } catch {
            case e: DataParseException =>
              throw new DataParseException(
                s"dim not found in field [${field.name}] params: ${e.getMessage}"
              )
          }
        case _ => 0
      }

      // Get element type for ArrayOfVector
      val elementType = if (field.dataType == DataType.ArrayOfVector) {
        field.elementType
      } else {
        DataType.None
      }

      val arrowType = DataTypeUtil.toArrowType(dim, field.dataType)

      // Create Arrow field
      val arrowField = if (field.dataType == DataType.ArrayOfVector) {
        // Add extra metadata for ArrayOfVector
        val metadata = Map(
          "PARQUET:field_id" -> field.fieldID.toString,
          "elementType" -> elementType.value.toString,
          "dim" -> dim.toString
        ).asJava

        val fieldType = new FieldType(
          true, // nullable
          arrowType,
          null, // dictionary encoding
          metadata
        )

        new Field(
          field.name,
          // field.fieldID.toString,
          fieldType,
          null // children
        )
      } else {
        convertToArrowField(field, arrowType)
      }

      arrowFields += arrowField
    }

    // Process all fields in the collection schema
    collectionSchema.fields.foreach { field =>
      appendArrowField(field)
    }

    // Create and return Arrow Schema
    new org.apache.arrow.vector.types.pojo.Schema(arrowFields.asJava)
  }

  /**
   * Convert Milvus CollectionSchema to Arrow Schema using field IDs as field names.
   * This is required for milvus-storage reader which matches columns by field ID.
   *
   * The manifest stores column groups with field IDs (e.g., "100", "101"),
   * so the Arrow schema must use field IDs as field names for the reader to
   * correctly match requested columns with column groups.
   *
   * Note: System fields (row_id, timestamp) are NOT included here.
   * They are handled by MilvusPartitionReaderFactory.
   *
   * @param collectionSchema The Milvus collection schema
   * @return Arrow Schema with field IDs as field names
   */
  def convertToArrowSchemaWithFieldIdNames(
      collectionSchema: io.milvus.grpc.schema.CollectionSchema
  ): org.apache.arrow.vector.types.pojo.Schema = {
    import scala.collection.JavaConverters._
    import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}

    val arrowFields = scala.collection.mutable.ArrayBuffer[Field]()

    // Helper function to create Arrow field with fieldID as name
    def appendArrowFieldWithIdName(field: FieldSchema): Unit = {
      // Get dimension for vector types
      val dim = field.dataType match {
        case DataType.BinaryVector | DataType.Float16Vector |
             DataType.BFloat16Vector | DataType.Int8Vector |
             DataType.FloatVector | DataType.ArrayOfVector =>
          try {
            getDim(field)
          } catch {
            case e: DataParseException =>
              throw new DataParseException(
                s"dim not found in field [${field.name}] params: ${e.getMessage}"
              )
          }
        case _ => 0
      }

      val arrowType = DataTypeUtil.toArrowType(dim, field.dataType)

      // Create metadata with both field_id and original name for reference
      val metadata = Map(
        "PARQUET:field_id" -> field.fieldID.toString,
        "original_name" -> field.name
      ).asJava

      val fieldType = new FieldType(
        true, // nullable
        arrowType,
        null, // dictionary encoding
        metadata
      )

      // Use fieldID.toString as the field name
      val arrowField = new Field(
        field.fieldID.toString,
        fieldType,
        null // children
      )

      arrowFields += arrowField
    }

    // Process all fields in the collection schema
    collectionSchema.fields.foreach { field =>
      appendArrowFieldWithIdName(field)
    }

    // Create and return Arrow Schema
    new org.apache.arrow.vector.types.pojo.Schema(arrowFields.asJava)
  }

  /**
   * Convert Spark StructType to Arrow Schema
   * This enables direct DataFrame to Arrow conversion without Milvus schema
   *
   * @param sparkSchema The Spark StructType schema
   * @param vectorDimensions Optional map of field name to vector dimension (for float arrays as vectors)
   * @return Arrow Schema
   */
  def convertSparkSchemaToArrow(
      sparkSchema: org.apache.spark.sql.types.StructType,
      vectorDimensions: Map[String, Int] = Map.empty,
      fieldIds: Map[String, Long] = Map.empty
  ): org.apache.arrow.vector.types.pojo.Schema = {
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.types._
    import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
    import org.apache.arrow.vector.types.FloatingPointPrecision

    val fields = sparkSchema.fields.zipWithIndex.map { case (field, idx) =>
      val arrowType: ArrowType = field.dataType match {
        case LongType => new ArrowType.Int(64, true)
        case IntegerType => new ArrowType.Int(32, true)
        case ShortType => new ArrowType.Int(16, true)
        case ByteType => new ArrowType.Int(8, true)
        case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
        case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
        case BooleanType => new ArrowType.Bool()
        case StringType => new ArrowType.Utf8()
        case BinaryType => new ArrowType.Binary()
        case ArrayType(FloatType, _) =>
          // Check if this field is a vector (has dimension specified)
          vectorDimensions.get(field.name) match {
            case Some(dim) =>
              // FloatVector as FixedSizeBinary (dim * 4 bytes)
              new ArrowType.FixedSizeBinary(dim * 4)
            case None =>
              // Regular float array as List
              new ArrowType.List()
          }
        case ArrayType(IntegerType, _) => new ArrowType.List()
        case ArrayType(LongType, _) => new ArrowType.List()
        case ArrayType(DoubleType, _) => new ArrowType.List()
        case ArrayType(StringType, _) => new ArrowType.List()
        case ArrayType(_, _) => new ArrowType.List()
        case MapType(_, _, _) => new ArrowType.Map(false)
        case StructType(_) => new ArrowType.Struct()
        case _ =>
          throw new IllegalArgumentException(s"Unsupported Spark type: ${field.dataType}")
      }

      // Use explicit field ID if provided, otherwise fall back to idx + 1
      val fieldId = fieldIds.getOrElse(field.name, (idx + 1).toLong)
      val metadata = Map("PARQUET:field_id" -> fieldId.toString).asJava

      val fieldType = new FieldType(true, arrowType, null, metadata)
      // Use field ID as column name when explicit field IDs are provided
      val fieldName = if (fieldIds.contains(field.name)) fieldId.toString else field.name
      new Field(fieldName, fieldType, null)
    }

    new org.apache.arrow.vector.types.pojo.Schema(fields.toList.asJava)
  }
}