package com.zilliz.spark.connector

import com.zilliz.milvus.storage.schema.ArrowTypes
import com.zilliz.spark.connector.serde.ArrowConverter

/** Spark StructType 到 Arrow Schema。Milvus CollectionSchema 到 Arrow Schema 的
  * 那一半已搬到 core 的 com.zilliz.milvus.storage.schema.SchemaMapper。
  */
object MilvusSchemaUtil {

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
          else ArrowTypes.toArrowType(dim, milvusType)
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
