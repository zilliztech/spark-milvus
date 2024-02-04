package zilliztech.spark.milvus.writer

import org.apache.spark.sql.connector.write.{Write, WriteBuilder}
import zilliztech.spark.milvus.MilvusOptions

case class MilvusWriteBuilder(milvusOptions: MilvusOptions)
  extends WriteBuilder
    with Serializable {
  override def build: Write = MilvusWrite(milvusOptions)
}
