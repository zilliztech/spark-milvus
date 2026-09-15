package com.zilliz.spark.connector.types

import org.apache.spark.sql.types.{DataType => SparkDataType}
import org.apache.spark.sql.types.{DataTypes, MetadataBuilder}

import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.schema.MilvusTypes
import com.zilliz.milvus.storage.DataParseException
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

/** Milvus type to Spark type. The Arrow half of the mapping lives in core, in
  * com.zilliz.milvus.storage.schema.ArrowTypes。
  */
object SparkTypes {

  def metadata(fieldSchema: FieldSchema) = {
    val builder = new MetadataBuilder()
      .putLong(
        FieldMetadata.MilvusDataTypeMetadataKey,
        fieldSchema.dataType.value
      )

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

  def toDataType(fieldSchema: FieldSchema): SparkDataType = {
    val dataType = fieldSchema.dataType
    dataType match {
      case MilvusDataType.Bool    => DataTypes.BooleanType
      case MilvusDataType.Int8    => DataTypes.ByteType
      case MilvusDataType.Int16   => DataTypes.ShortType
      case MilvusDataType.Int32   => DataTypes.IntegerType
      case MilvusDataType.Int64   => DataTypes.LongType
      case MilvusDataType.Float   => DataTypes.FloatType
      case MilvusDataType.Double  => DataTypes.DoubleType
      case MilvusDataType.String  => DataTypes.StringType
      case MilvusDataType.VarChar => DataTypes.StringType
      case MilvusDataType.JSON    => DataTypes.StringType
      case MilvusDataType.Array =>
        val elementType = fieldSchema.elementType
        val sparkElementType = elementType match {
          case MilvusDataType.Bool    => DataTypes.BooleanType
          case MilvusDataType.Int8    => DataTypes.ShortType
          case MilvusDataType.Int16   => DataTypes.ShortType
          case MilvusDataType.Int32   => DataTypes.IntegerType
          case MilvusDataType.Int64   => DataTypes.LongType
          case MilvusDataType.Float   => DataTypes.FloatType
          case MilvusDataType.Double  => DataTypes.DoubleType
          case MilvusDataType.String  => DataTypes.StringType
          case MilvusDataType.VarChar => DataTypes.StringType
          case _ =>
            throw new DataParseException(
              s"Unsupported Milvus data element type: $elementType"
            )
        }
        DataTypes.createArrayType(sparkElementType)
      case MilvusDataType.Geometry =>
        DataTypes.createArrayType(
          DataTypes.BinaryType
        ) // TODO: fubang support geometry
      case MilvusDataType.FloatVector =>
        DataTypes.createArrayType(DataTypes.FloatType)
      case MilvusDataType.BinaryVector =>
        DataTypes.BinaryType
      case MilvusDataType.Int8Vector =>
        DataTypes.createArrayType(DataTypes.ShortType)
      case MilvusDataType.Float16Vector =>
        DataTypes.createArrayType(DataTypes.FloatType)
      case MilvusDataType.BFloat16Vector =>
        DataTypes.createArrayType(DataTypes.FloatType)
      case MilvusDataType.SparseFloatVector =>
        DataTypes.createMapType(DataTypes.LongType, DataTypes.FloatType)
      case _ =>
        throw new DataParseException(
          s"Unsupported Milvus data type: $dataType"
        )
    }
  }
}
