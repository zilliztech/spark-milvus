package com.zilliz.spark.connector.types

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.spark.sql.types.{
  ArrayType,
  DataTypes,
  MetadataBuilder,
  StructField
}
import org.apache.spark.sql.types.{DataType => SparkDataType}

import com.zilliz.milvus.storage.schema.{ArrowTypes, FieldMetadata, MilvusTypes}
import com.zilliz.milvus.storage.DataParseException
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

/** Arrow type to Spark type, and the field metadata a Milvus field carries into
  * a Spark schema. The Milvus-to-Arrow half of the mapping is core's
  * `com.zilliz.milvus.storage.schema.ArrowTypes`; a field's Spark type is the
  * composition of the two (capability R15).
  */
object SparkTypes {

  /** The complete Spark field exposed for one Milvus field. Type, nullability
    * and Milvus-owned metadata come from the same FieldSchema so callers cannot
    * accidentally maintain independent mappings.
    */
  def toStructField(
      fieldSchema: FieldSchema,
      rawVectors: Boolean = false
  ): StructField =
    StructField(
      fieldSchema.name,
      toDataType(fieldSchema, rawVectors),
      nullable = fieldSchema.nullable,
      metadata = metadata(fieldSchema)
    )

  def metadata(fieldSchema: FieldSchema) = {
    val builder = new MetadataBuilder()
      .putLong(
        FieldMetadata.MilvusDataTypeMetadataKey,
        fieldSchema.dataType.value
      )
      .putLong(FieldMetadata.MilvusFieldIdMetadataKey, fieldSchema.fieldID)
    if (fieldSchema.isPrimaryKey) {
      builder.putBoolean(FieldMetadata.MilvusPrimaryKeyMetadataKey, true)
    }
    if (fieldSchema.isPartitionKey) {
      builder.putBoolean(FieldMetadata.MilvusPartitionKeyMetadataKey, true)
    }
    if (fieldSchema.isClusteringKey) {
      builder.putBoolean(FieldMetadata.MilvusClusteringKeyMetadataKey, true)
    }

    if (MilvusTypes.isDenseVectorType(fieldSchema.dataType)) {
      fieldSchema.typeParams
        .find(_.key == "dim")
        .foreach { param =>
          builder.putLong(
            FieldMetadata.MilvusVectorDimensionMetadataKey,
            MilvusTypes.parseVectorDimension(fieldSchema.name, param.value)
          )
        }
    }

    builder.build()
  }

  /** The Spark type a field is presented as.
    *
    * @param rawVectors
    *   `milvus.read.vector.raw`: hand vector columns over as the bytes Milvus
    *   stored rather than decoding them. Scalar columns are unaffected.
    */
  def toDataType(
      fieldSchema: FieldSchema,
      rawVectors: Boolean
  ): SparkDataType =
    if (rawVectors && MilvusTypes.isVectorType(fieldSchema.dataType)) {
      DataTypes.BinaryType
    } else {
      toDataType(fieldSchema)
    }

  /** The Spark type a field is presented as: its Milvus type becomes an Arrow
    * type through core's `ArrowTypes`, and that Arrow type comes to
    * [[fromArrow]]. There is no direct Milvus-to-Spark table.
    */
  def toDataType(fieldSchema: FieldSchema): SparkDataType = {
    // The Arrow kind is what decides the Spark type; the byte width it is
    // built with does not matter here, so a field without a dim maps too.
    val dim = fieldSchema.typeParams
      .find(_.key == "dim")
      .map(p =>
        MilvusTypes.parseVectorDimension(fieldSchema.name, p.value).toInt
      )
      .getOrElse(0)
    fromArrow(
      ArrowTypes.toArrowType(dim, fieldSchema.dataType),
      fieldSchema.dataType,
      fieldSchema.elementType
    )
  }

  /** Arrow type to Spark type, keyed by the Arrow type and the Milvus logical
    * type. The second key cannot be dropped: Milvus stores JSON, Array,
    * Geometry and sparse vectors all as Arrow Binary and every dense vector as
    * FixedSizeBinary, so the Arrow type alone cannot tell a JSON string from a
    * sparse vector or a float vector from a float16 one.
    *
    * @param elementType
    *   the element type of a Milvus Array; ignored for every other field
    */
  def fromArrow(
      arrowType: ArrowType,
      milvusType: MilvusDataType,
      elementType: MilvusDataType = MilvusDataType.None
  ): SparkDataType = (arrowType, milvusType) match {
    case (_: ArrowType.Bool, MilvusDataType.Bool) => DataTypes.BooleanType
    case (int: ArrowType.Int, MilvusDataType.Int8) if int.getBitWidth == 8 =>
      DataTypes.ByteType
    case (int: ArrowType.Int, MilvusDataType.Int16) if int.getBitWidth == 16 =>
      DataTypes.ShortType
    case (int: ArrowType.Int, MilvusDataType.Int32) if int.getBitWidth == 32 =>
      DataTypes.IntegerType
    case (int: ArrowType.Int, MilvusDataType.Int64) if int.getBitWidth == 64 =>
      DataTypes.LongType
    case (float: ArrowType.FloatingPoint, MilvusDataType.Float)
        if float.getPrecision == FloatingPointPrecision.SINGLE =>
      DataTypes.FloatType
    case (float: ArrowType.FloatingPoint, MilvusDataType.Double)
        if float.getPrecision == FloatingPointPrecision.DOUBLE =>
      DataTypes.DoubleType
    case (
          _: ArrowType.Utf8,
          MilvusDataType.String | MilvusDataType.VarChar | MilvusDataType.Text
        ) =>
      DataTypes.StringType
    case (_: ArrowType.Binary, MilvusDataType.JSON) =>
      DataTypes.StringType
    case (_: ArrowType.Binary, MilvusDataType.Array) =>
      DataTypes.createArrayType(arrayElementType(elementType))
    case (_: ArrowType.Binary, MilvusDataType.SparseFloatVector) =>
      DataTypes.createMapType(DataTypes.LongType, DataTypes.FloatType)
    case (_: ArrowType.FixedSizeBinary, _)
        if MilvusTypes.isDenseVectorType(milvusType) =>
      // The column presents its elements as non-nullable, which is true of a
      // stored vector; the table schema has always said containsNull = true,
      // and WriteSchema compares types ignoring nullability.
      MilvusVectorColumn.sparkType(milvusType, raw = false) match {
        case ArrayType(element, _) => DataTypes.createArrayType(element)
        case other                 => other
      }
    case (other, _) =>
      throw new DataParseException(
        s"Unsupported Milvus data type: $milvusType (stored as Arrow $other)"
      )
  }

  /** The element type of a Milvus Array: the scalar types, and nothing else.
    * Int8 elements are presented as ShortType, as they always have been; the
    * decoders in ArrowConverter and MilvusArrayColumn read either width.
    */
  private def arrayElementType(elementType: MilvusDataType): SparkDataType =
    elementType match {
      case MilvusDataType.Int8 => DataTypes.ShortType
      case MilvusDataType.Bool | MilvusDataType.Int16 | MilvusDataType.Int32 |
          MilvusDataType.Int64 | MilvusDataType.Float | MilvusDataType.Double |
          MilvusDataType.String | MilvusDataType.VarChar =>
        fromArrow(ArrowTypes.toArrowType(0, elementType), elementType)
      case other =>
        throw new DataParseException(
          s"Unsupported Milvus data element type: $other"
        )
    }
}
