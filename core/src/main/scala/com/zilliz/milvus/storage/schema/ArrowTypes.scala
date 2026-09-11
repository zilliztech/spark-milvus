package com.zilliz.milvus.storage.schema

import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.FloatingPointPrecision

import com.zilliz.milvus.storage.DataParseException
import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** Milvus 类型到 Arrow 类型的唯一映射。不含 Spark 类型，那一层的映射在 spark 模块。 */
object ArrowTypes {

  /** @param dim
    *   稠密向量的维度，非向量字段传 0。定长向量的字节宽度由它算出来。
    */
  def toArrowType(dim: Int, dataType: MilvusDataType): ArrowType = {
    dataType match {
      case MilvusDataType.Bool  => new ArrowType.Bool()
      case MilvusDataType.Int8  => new ArrowType.Int(8, true)
      case MilvusDataType.Int16 => new ArrowType.Int(16, true)
      case MilvusDataType.Int32 => new ArrowType.Int(32, true)
      case MilvusDataType.Int64 => new ArrowType.Int(64, true)
      case MilvusDataType.Float =>
        new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
      case MilvusDataType.Double =>
        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
      case MilvusDataType.Timestamptz => new ArrowType.Int(64, true)
      case MilvusDataType.VarChar | MilvusDataType.String |
          MilvusDataType.Text =>
        new ArrowType.Utf8()
      case MilvusDataType.Array | MilvusDataType.JSON |
          MilvusDataType.Geometry =>
        new ArrowType.Binary()
      case MilvusDataType.BinaryVector =>
        new ArrowType.FixedSizeBinary((dim + 7) / 8)
      case MilvusDataType.Float16Vector =>
        new ArrowType.FixedSizeBinary(dim * 2)
      case MilvusDataType.BFloat16Vector =>
        new ArrowType.FixedSizeBinary(dim * 2)
      case MilvusDataType.Int8Vector  => new ArrowType.FixedSizeBinary(dim)
      case MilvusDataType.FloatVector => new ArrowType.FixedSizeBinary(dim * 4)
      case MilvusDataType.SparseFloatVector => new ArrowType.Binary()
      case MilvusDataType.ArrayOfVector     => new ArrowType.List()
      case _ =>
        throw new DataParseException(
          s"Unsupported Milvus data type for Arrow conversion: $dataType"
        )
    }
  }
}
