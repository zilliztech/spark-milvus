package zilliztech.spark.milvus.writer

import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import org.apache.spark.sql.types.StructType
import zilliztech.spark.milvus.MilvusOptions

case class MilvusWrite(milvusOptions: MilvusOptions, schema: StructType) extends Write with Serializable {
  override def toBatch: BatchWrite = MilvusBatchWriter(milvusOptions, schema)
}
