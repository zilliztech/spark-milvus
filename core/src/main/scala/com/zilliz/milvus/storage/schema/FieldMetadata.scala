package com.zilliz.milvus.storage.schema

/** 字段元数据的键名。
  *
  * Milvus 的类型信息在 Arrow 和 Spark 的 schema 里都没有对应表示：Arrow 只知道 一列是
  * FixedSizeBinary，Spark 只知道它是 BinaryType，谁也不知道它是维度 128 的 FloatVector。这两个键就是把
  * Milvus 类型和维度挂在字段上带下去的地方， 读写两侧、Arrow 侧和 Spark 侧共用同一套键名，所以它们属于 core。
  */
object FieldMetadata {

  /** Milvus DataType 的枚举值，按 Long 存。 */
  val MilvusDataTypeMetadataKey = "milvus.data_type"

  /** 稠密向量的维度，按 Long 存。可空稠密向量落变长二进制时靠它还原宽度。 */
  val MilvusVectorDimensionMetadataKey = "milvus.vector_dim"
}
