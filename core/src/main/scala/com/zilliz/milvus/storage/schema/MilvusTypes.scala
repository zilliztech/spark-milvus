package com.zilliz.milvus.storage.schema

import com.zilliz.milvus.storage.DataParseException
import io.milvus.grpc.schema.{DataType => MilvusDataType, FieldSchema}

/** Milvus 字段类型的判定与取维。
  *
  * 存储格式里的 schema 就是 schema.proto 的 CollectionSchema，core 不另建一套
  * 模型，只在它上面提供判定，避免两份真相。
  */
object MilvusTypes {

  /** 按定长二进制落盘的稠密向量。SparseFloatVector 不在其中，它落变长二进制。 */
  val DenseVectorTypes: Set[MilvusDataType] = Set(
    MilvusDataType.FloatVector,
    MilvusDataType.BinaryVector,
    MilvusDataType.Float16Vector,
    MilvusDataType.BFloat16Vector,
    MilvusDataType.Int8Vector
  )

  def isDenseVectorType(dataType: MilvusDataType): Boolean =
    DenseVectorTypes.contains(dataType)

  /** 向量字段都带 dim，且必须落进 Int 范围：Arrow 的 FixedSizeBinary 宽度是 int。 */
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

  /** 从 typeParams 里取 dim；没有就是 schema 有问题，不给默认值。 */
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
