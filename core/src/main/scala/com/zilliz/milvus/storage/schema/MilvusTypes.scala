package com.zilliz.milvus.storage.schema

import com.zilliz.milvus.storage.DataParseException
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

/** Predicates over Milvus field types, and reading a field's dimension.
  *
  * The schema in the storage format is exactly the CollectionSchema from
  * schema.proto. Core does not build a parallel model of it; it only adds
  * predicates on top, so there is one source of truth rather than two.
  */
object MilvusTypes {

  /** Dense vectors, which land as fixed-size binary. SparseFloatVector is not
    * one of them: it lands as variable-width binary.
    */
  val DenseVectorTypes: Set[MilvusDataType] = Set(
    MilvusDataType.FloatVector,
    MilvusDataType.BinaryVector,
    MilvusDataType.Float16Vector,
    MilvusDataType.BFloat16Vector,
    MilvusDataType.Int8Vector
  )

  def isDenseVectorType(dataType: MilvusDataType): Boolean =
    DenseVectorTypes.contains(dataType)

  /** Every vector type, dense or sparse. What `milvus.read.vector.raw` acts on:
    * a sparse vector is stored as bytes too, so raw mode hands those over
    * unchanged just the same.
    */
  def isVectorType(dataType: MilvusDataType): Boolean =
    isDenseVectorType(dataType) ||
      dataType == MilvusDataType.SparseFloatVector

  /** Every vector field carries a dim, and it has to fit in an Int: the width
    * of Arrow's FixedSizeBinary is an int.
    */
  def parseVectorDimension(fieldName: String, rawDimension: String): Long = {
    val dimension =
      try rawDimension.toLong
      catch {
        case _: NumberFormatException =>
          throw new DataParseException(
            s"Invalid vector dimension '$rawDimension' for field '$fieldName'"
          )
      }
    if (dimension <= 0 || dimension > Int.MaxValue) {
      throw new DataParseException(
        s"Invalid vector dimension $dimension for field '$fieldName'"
      )
    }
    dimension
  }

  /** Reads dim out of typeParams. A missing dim means the schema is wrong, so
    * there is no default.
    */
  def dimensionOf(fieldSchema: FieldSchema): Int = {
    fieldSchema.typeParams
      .find(_.key == "dim")
      .map(param => parseVectorDimension(fieldSchema.name, param.value).toInt)
      .getOrElse(
        throw new DataParseException(
          s"Field ${fieldSchema.name} has no dim parameter"
        )
      )
  }
}
