package com.zilliz.spark.connector.types

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.spark.sql.types.{
  ArrayType,
  DataTypes,
  MetadataBuilder,
  StructField,
  StructType
}
import org.apache.spark.sql.types.{DataType => SparkDataType}

import com.zilliz.milvus.storage.schema.{ArrowTypes, FieldMetadata, MilvusTypes}
import com.zilliz.milvus.storage.DataParseException
import io.milvus.grpc.schema.{
  DataType => MilvusDataType,
  FieldSchema,
  StructArrayFieldSchema
}

/** Arrow type to Spark type, and the field metadata a Milvus field carries into
  * a Spark schema. The Milvus-to-Arrow half of the mapping is core's
  * `com.zilliz.milvus.storage.schema.ArrowTypes`; a field's Spark type is the
  * composition of the two.
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

  /** One Spark field per logical struct-array parent. The stored child names
    * may be qualified as parent[member]; Spark exposes the member name.
    */
  def toStructArrayField(
      parent: StructArrayFieldSchema,
      rawVectors: Boolean
  ): StructField = {
    val members = parent.fields.map { child =>
      val name =
        if (
          child.name.startsWith(parent.name + "[") && child.name.endsWith("]")
        )
          child.name.substring(parent.name.length + 1, child.name.length - 1)
        else child.name
      require(
        name.nonEmpty && !name.exists(c => c == '[' || c == ']'),
        s"Invalid member name '${child.name}' in struct array '${parent.name}'"
      )
      val memberType = child.dataType match {
        case MilvusDataType.ArrayOfVector
            if MilvusTypes.isDenseVectorType(child.elementType) =>
          MilvusTypes.dimensionOf(child)
          MilvusVectorColumn.sparkType(child.elementType, rawVectors)
        case MilvusDataType.Array
            if child.elementType == MilvusDataType.Array =>
          val leaf = MilvusTypes.nestedArrayLeaf(child)
          ArrayType(scalarMemberType(leaf.getLeafType), leaf.nullable)
        case MilvusDataType.Array => scalarMemberType(child.elementType)
        case other =>
          throw new DataParseException(
            s"Struct array '${parent.name}' has unsupported member '${child.name}' of type $other"
          )
      }
      StructField(name, memberType, child.elementNullable, metadata(child))
    }
    require(members.nonEmpty, s"Struct array '${parent.name}' has no members")
    require(
      members
        .map(_.name.toLowerCase(java.util.Locale.ROOT))
        .distinct
        .size == members.size,
      s"Struct array '${parent.name}' contains duplicate member names"
    )
    val parentMetadata = new MetadataBuilder()
      .putLong(
        FieldMetadata.MilvusDataTypeMetadataKey,
        MilvusDataType.ArrayOfStruct.value
      )
      .putLong(FieldMetadata.MilvusFieldIdMetadataKey, parent.fieldID)
      .build()
    StructField(
      parent.name,
      ArrayType(StructType(members), containsNull = false),
      nullable = parent.nullable,
      metadata = parentMetadata
    )
  }

  private def scalarMemberType(elementType: MilvusDataType): SparkDataType =
    if (elementType == MilvusDataType.Int8) DataTypes.ByteType
    else arrayElementType(elementType)

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

    if (
      MilvusTypes.isDenseVectorType(
        fieldSchema.dataType
      ) || fieldSchema.dataType == MilvusDataType.ArrayOfVector
    ) {
      fieldSchema.typeParams
        .find(_.key == "dim")
        .foreach { param =>
          builder.putLong(
            FieldMetadata.MilvusVectorDimensionMetadataKey,
            MilvusTypes.parseVectorDimension(fieldSchema.name, param.value)
          )
        }
    }

    // An Array's element type goes to the writer: Int8 and Int16 elements
    // share Int32's Spark and physical form, and only the element type bounds
    // them as the Milvus proxy does on insert.
    if (
      fieldSchema.dataType == MilvusDataType.Array || fieldSchema.dataType == MilvusDataType.ArrayOfVector
    ) {
      builder.putLong(
        FieldMetadata.MilvusElementTypeMetadataKey,
        fieldSchema.elementType.value.toLong
      )
    }

    builder.build()
  }

  /** The Spark type a field is presented as.
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
    * MilvusArrayColumn presents both Int8 and Int16 elements as ShortType. The
    * writer encodes both as IntData, with bounds from the element type in the
    * field metadata.
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
