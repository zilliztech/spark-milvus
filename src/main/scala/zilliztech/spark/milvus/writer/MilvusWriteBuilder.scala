package zilliztech.spark.milvus.writer

import org.apache.spark.sql.connector.write.{Write, WriteBuilder, LogicalWriteInfo}
import zilliztech.spark.milvus.MilvusOptions

case class MilvusWriteBuilder(milvusOptions: MilvusOptions, info: LogicalWriteInfo)
  extends WriteBuilder
    with Serializable {
  override def build: Write = MilvusWrite(milvusOptions, info.schema())
}
