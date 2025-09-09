package com.zilliz.spark.connector

import org.apache.spark.sql.types.{DataType => SparkDataType}
import org.apache.spark.sql.types.DataTypes

import com.zilliz.spark.connector.DataParseException
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

object DataTypeUtil {
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
        DataTypes.createArrayType(DataTypes.BinaryType)
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
