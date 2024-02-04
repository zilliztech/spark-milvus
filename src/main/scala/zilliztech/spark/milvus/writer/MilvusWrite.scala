package zilliztech.spark.milvus.writer

import org.apache.spark.sql.connector.write.{BatchWrite, Write}
import zilliztech.spark.milvus.MilvusOptions

case class MilvusWrite(milvusOptions: MilvusOptions) extends Write with Serializable {
  override def toBatch: BatchWrite = MilvusBatchWriter(milvusOptions)
}
