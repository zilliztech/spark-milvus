package com.zilliz.milvus.storage.schema

import io.milvus.grpc.schema.{DataType => MilvusDataType}

/** The element type of a dense vector column. */
sealed trait VectorElementType

object VectorElementType {
  case object Float32 extends VectorElementType
  case object Float16 extends VectorElementType
  case object BFloat16 extends VectorElementType
  case object Int8 extends VectorElementType

  /** One dimension per bit, eight dimensions per byte. */
  case object Bit extends VectorElementType
}

/** How one dense vector column's values sit in memory: the element type, the
  * dimension, and the bytes one row occupies. Milvus types map onto it through
  * `of`; other formats fill it from their own types
  * (docs/design/architecture/read.html section 6.4).
  */
final case class VectorLayout(
    elementType: VectorElementType,
    dimension: Int
) {
  import VectorElementType._

  require(dimension > 0, s"Vector dimension must be positive: $dimension")
  require(
    elementType != Bit || dimension % 8 == 0,
    s"A binary vector dimension must be a multiple of 8: $dimension"
  )

  /** The width of one element, and the alignment the data buffer needs. */
  def elementBytes: Int = elementType match {
    case Float32            => 4
    case Float16 | BFloat16 => 2
    case Int8 | Bit         => 1
  }

  def rowBytes: Int = elementType match {
    case Bit   => dimension / 8
    case other => Math.multiplyExact(dimension, elementBytes)
  }
}

object VectorLayout {

  /** The layout Milvus writes for a dense vector field. */
  def of(dataType: MilvusDataType, dimension: Int): VectorLayout = {
    val elementType = dataType match {
      case MilvusDataType.FloatVector    => VectorElementType.Float32
      case MilvusDataType.Float16Vector  => VectorElementType.Float16
      case MilvusDataType.BFloat16Vector => VectorElementType.BFloat16
      case MilvusDataType.Int8Vector     => VectorElementType.Int8
      case MilvusDataType.BinaryVector   => VectorElementType.Bit
      case other =>
        throw new IllegalArgumentException(
          s"$other is not a dense vector type"
        )
    }
    VectorLayout(elementType, dimension)
  }
}
