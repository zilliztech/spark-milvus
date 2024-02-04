package zilliztech.spark.milvus.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import zilliztech.spark.milvus.MilvusOptions

case class MilvusDataWriterFactory(milvusOptions: MilvusOptions)
  extends DataWriterFactory
    with Serializable {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    MilvusDataWriter(partitionId, taskId, milvusOptions)
  }
}
