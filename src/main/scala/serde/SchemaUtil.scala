package com.zilliz.spark.connector

import com.zilliz.spark.connector.serde.ArrowConverter
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

/** MilvusSchemaUtil provides utilities for converting between Milvus and Arrow
  * schemas
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

  private[connector] val CanonicalSystemFields: Seq[FieldSchema] = Seq(
    FieldSchema(name = "RowID", fieldID = 0, dataType = DataType.Int64),
    FieldSchema(name = "Timestamp", fieldID = 1, dataType = DataType.Int64)
  )

  private[connector] def systemFieldNameAliases(
      field: FieldSchema
  ): Set[String] = {
    field.fieldID match {
      case 0 => Set("rowid", "row_id")
      case 1 => Set("timestamp")
      case _ => Set(field.name.toLowerCase)
    }
  }

  private[connector] def missingSystemFields(
      collectionSchema: io.milvus.grpc.schema.CollectionSchema
  ): Seq[FieldSchema] = {
    val existingNames = collectionSchema.fields.map(_.name.toLowerCase).toSet
    val existingFieldIds = collectionSchema.fields.map(_.fieldID).toSet
    CanonicalSystemFields.filterNot(field =>
      systemFieldNameAliases(field).exists(existingNames.contains) ||
        existingFieldIds.contains(field.fieldID)
    )
  }

  /** Convert Milvus FieldSchema to Arrow Field
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

  /** Convert Milvus CollectionSchema to Arrow Schema This function converts a
    * Milvus collection schema to an Arrow schema format. Now uses the serdeMap
    * for consistent type conversion.
    *
    * @param collectionSchema
    *   The Milvus collection schema
    * @return
    *   Arrow Schema
    */
  def convertToArrowSchema(
      collectionSchema: io.milvus.grpc.schema.CollectionSchema
  ): org.apache.arrow.vector.types.pojo.Schema = {
    import scala.collection.JavaConverters._
    import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}

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
      } else if (
        field.dataType == DataType.BinaryVector ||
        field.dataType == DataType.FloatVector ||
        field.dataType == DataType.Float16Vector ||
        field.dataType == DataType.BFloat16Vector ||
        field.dataType == DataType.Int8Vector ||
        field.dataType == DataType.SparseFloatVector
      ) {
        val isDense = field.dataType != DataType.SparseFloatVector
        val physicalType =
          if (isDense && field.nullable) new ArrowType.Binary() else arrowType
        val metadata = (Map(
          "PARQUET:field_id" -> field.fieldID.toString
        ) ++
          (if (isDense && field.nullable) Map("dim" -> dim.toString)
           else Map.empty)).asJava
        new Field(
          field.name,
          new FieldType(
            field.nullable,
            physicalType,
            null,
            metadata
          ),
          null
        )
      } else {
        convertToArrowField(field, arrowType)
      }

      arrowFields += arrowField
    }

    (missingSystemFields(collectionSchema) ++ collectionSchema.fields).foreach {
      field =>
        appendArrowField(field)
    }

    // Create and return Arrow Schema
    new org.apache.arrow.vector.types.pojo.Schema(arrowFields.asJava)
  }

  /** Convert Milvus CollectionSchema to Arrow Schema using field IDs as field
    * names. This is required for milvus-storage reader which matches columns by
    * field ID.
    *
    * The manifest stores column groups with field IDs (e.g., "100", "101"), so
    * the Arrow schema must use field IDs as field names for the reader to
    * correctly match requested columns with column groups.
    *
    * System fields (RowID=0, Timestamp=1) are included when the Milvus schema
    * does not already declare them, so readers can project system fields
    * consistently.
    *
    * @param collectionSchema
    *   The Milvus collection schema
    * @return
    *   Arrow Schema with field IDs as field names
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
      val isDenseVector = field.dataType match {
        case DataType.BinaryVector | DataType.FloatVector |
            DataType.Float16Vector | DataType.BFloat16Vector |
            DataType.Int8Vector =>
          true
        case _ => false
      }
      val isVector =
        isDenseVector || field.dataType == DataType.SparseFloatVector
      val physicalType =
        if (isDenseVector && field.nullable) new ArrowType.Binary()
        else arrowType

      // Create metadata with both field_id and original name for reference
      val metadata = Map(
        "PARQUET:field_id" -> field.fieldID.toString,
        "original_name" -> field.name
      ) ++
        (if (isDenseVector && field.nullable) Map("dim" -> dim.toString)
         else Map.empty)

      val fieldType = new FieldType(
        if (isVector) field.nullable else true,
        physicalType,
        null, // dictionary encoding
        metadata.asJava
      )

      // Use fieldID.toString as the field name
      val arrowField = new Field(
        field.fieldID.toString,
        fieldType,
        null // children
      )

      arrowFields += arrowField
    }

    (missingSystemFields(collectionSchema) ++ collectionSchema.fields).foreach {
      field =>
        appendArrowFieldWithIdName(field)
    }

    // Create and return Arrow Schema
    new org.apache.arrow.vector.types.pojo.Schema(arrowFields.asJava)
  }

  /** Convert Spark StructType to Arrow Schema This enables direct DataFrame to
    * Arrow conversion without Milvus schema
    *
    * @param sparkSchema
    *   The Spark StructType schema
    * @param vectorDimensions
    *   Optional map of field name to vector dimension (for float arrays as
    *   vectors)
    * @param fieldIds
    *   Optional map of field name -> Milvus fieldID. When non-empty, each
    *   matched field carries a `PARQUET:field_id` metadata entry.
    * @param useFieldIdAsName
    *   When true (default, V3 writer semantics), fields with an explicit
    *   fieldID entry get their Arrow column name rewritten to the fieldID
    *   string. When false (V2 packed-parquet semantics — what milvus segcore
    *   produces), the Arrow column name stays as the Spark field's logical name
    *   while `PARQUET:field_id` metadata still carries the fieldID.
    * @return
    *   Arrow Schema
    */
  def convertSparkSchemaToArrow(
      sparkSchema: org.apache.spark.sql.types.StructType,
      vectorDimensions: Map[String, Int] = Map.empty,
      fieldIds: Map[String, Long] = Map.empty,
      useFieldIdAsName: Boolean = true
  ): org.apache.arrow.vector.types.pojo.Schema = {
    import scala.collection.JavaConverters._
    import org.apache.spark.sql.types._
    import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
    import org.apache.arrow.vector.types.FloatingPointPrecision
    import io.milvus.grpc.schema.{DataType => MilvusDataType}

    val denseVectorTypes: Set[MilvusDataType] = Set(
      MilvusDataType.FloatVector,
      MilvusDataType.BinaryVector,
      MilvusDataType.Float16Vector,
      MilvusDataType.BFloat16Vector,
      MilvusDataType.Int8Vector
    )
    val vectorTypes: Set[MilvusDataType] =
      denseVectorTypes + MilvusDataType.SparseFloatVector

    def milvusVectorType(field: StructField): Option[MilvusDataType] =
      Option(field.metadata)
        .filter(_.contains(ArrowConverter.MilvusDataTypeMetadataKey))
        .map(
          _.getLong(ArrowConverter.MilvusDataTypeMetadataKey).toInt
        )
        .map(MilvusDataType.fromValue)
        .filter(vectorTypes.contains)

    def vectorDimension(
        field: StructField,
        milvusType: MilvusDataType
    ): Int =
      if (!denseVectorTypes.contains(milvusType)) 0
      else {
        Option(field.metadata)
          .filter(
            _.contains(ArrowConverter.MilvusVectorDimensionMetadataKey)
          )
          .map(
            _.getLong(ArrowConverter.MilvusVectorDimensionMetadataKey).toInt
          )
          .orElse(vectorDimensions.get(field.name))
          .filter(_ > 0)
          .getOrElse {
            throw new IllegalArgumentException(
              s"Milvus vector field '${field.name}' ($milvusType) requires positive ${ArrowConverter.MilvusVectorDimensionMetadataKey} metadata or vectorDimensions entry"
            )
          }
      }

    val fields = sparkSchema.fields.zipWithIndex.map { case (field, idx) =>
      val vectorType = milvusVectorType(field)
      val dim = vectorType.map(vectorDimension(field, _)).getOrElse(0)
      val arrowType: ArrowType = vectorType match {
        case Some(milvusType) if denseVectorTypes.contains(milvusType) =>
          // Milvus uses variable-width Binary for nullable dense vectors so a
          // null row does not have to carry a fixed-width payload. Non-nullable
          // dense vectors use their exact FixedSizeBinary width.
          if (field.nullable) new ArrowType.Binary()
          else DataTypeUtil.toArrowType(dim, milvusType)
        case Some(MilvusDataType.SparseFloatVector) =>
          new ArrowType.Binary()
        case _ =>
          field.dataType match {
            case LongType    => new ArrowType.Int(64, true)
            case IntegerType => new ArrowType.Int(32, true)
            case ShortType   => new ArrowType.Int(16, true)
            case ByteType    => new ArrowType.Int(8, true)
            case FloatType =>
              new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
            case DoubleType =>
              new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
            case BooleanType             => new ArrowType.Bool()
            case StringType              => new ArrowType.Utf8()
            case BinaryType              => new ArrowType.Binary()
            case ArrayType(FloatType, _) =>
              // Legacy/general writer path: vector dimensions supplied via
              // options identify Array[Float] as FloatVector.
              vectorDimensions.get(field.name) match {
                case Some(vectorDim) =>
                  new ArrowType.FixedSizeBinary(vectorDim * 4)
                case None =>
                  new ArrowType.List()
              }
            case ArrayType(IntegerType, _) => new ArrowType.List()
            case ArrayType(LongType, _)    => new ArrowType.List()
            case ArrayType(DoubleType, _)  => new ArrowType.List()
            case ArrayType(StringType, _)  => new ArrowType.List()
            case ArrayType(_, _)           => new ArrowType.List()
            case MapType(_, _, _)          => new ArrowType.Map(false)
            case StructType(_)             => new ArrowType.Struct()
            case _ =>
              throw new IllegalArgumentException(
                s"Unsupported Spark type: ${field.dataType}"
              )
          }
      }

      // Use explicit field ID if provided, otherwise avoid Milvus system IDs 0/1.
      val fieldId = fieldIds.getOrElse(field.name, (idx + 100).toLong)
      val metadata = (Map("PARQUET:field_id" -> fieldId.toString) ++
        vectorType
          .filter(denseVectorTypes.contains)
          .filter(_ => field.nullable)
          .map(_ => Map("dim" -> dim.toString))
          .getOrElse(Map.empty)).asJava

      val arrowNullable = if (vectorType.isDefined) field.nullable else true
      val fieldType =
        new FieldType(arrowNullable, arrowType, null, metadata)
      val fieldName =
        if (useFieldIdAsName && fieldIds.contains(field.name))
          fieldId.toString
        else field.name
      new Field(fieldName, fieldType, null)
    }

    new org.apache.arrow.vector.types.pojo.Schema(fields.toList.asJava)
  }
}
