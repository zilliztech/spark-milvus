package com.zilliz.spark.connector

import org.apache.spark.sql.types.{DataType => SparkDataType}
import org.apache.spark.sql.types.DataTypes
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.FloatingPointPrecision

import com.zilliz.spark.connector.DataParseException
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

object DataTypeUtil {
  /**
   * Converts Milvus DataType to Arrow type given dimension and element type
   */
  def toArrowType(dim: Int, dataType: MilvusDataType): ArrowType = {
    dataType match {
      case MilvusDataType.Bool => new ArrowType.Bool()
      case MilvusDataType.Int8 => new ArrowType.Int(8, true)
      case MilvusDataType.Int16 => new ArrowType.Int(16, true)
      case MilvusDataType.Int32 => new ArrowType.Int(32, true)
      case MilvusDataType.Int64 => new ArrowType.Int(64, true)
      case MilvusDataType.Float => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case MilvusDataType.Double => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case MilvusDataType.Timestamptz => new ArrowType.Int(64, true)
      case MilvusDataType.VarChar | MilvusDataType.String | MilvusDataType.Text => new ArrowType.Utf8()
      case MilvusDataType.Array | MilvusDataType.JSON | MilvusDataType.Geometry => new ArrowType.Binary()
      case MilvusDataType.BinaryVector => new ArrowType.FixedSizeBinary((dim + 7) / 8)
      case MilvusDataType.Float16Vector => new ArrowType.FixedSizeBinary(dim * 2)
      case MilvusDataType.BFloat16Vector => new ArrowType.FixedSizeBinary(dim * 2)
      case MilvusDataType.Int8Vector => new ArrowType.FixedSizeBinary(dim)
      case MilvusDataType.FloatVector => new ArrowType.FixedSizeBinary(dim * 4)
      case MilvusDataType.SparseFloatVector => new ArrowType.Binary()
      case MilvusDataType.ArrayOfVector => new ArrowType.List()
      case _ => throw new DataParseException(s"Unsupported Milvus data type for Arrow conversion: $dataType")
    }
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
