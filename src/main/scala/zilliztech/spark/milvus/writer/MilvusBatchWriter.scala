package zilliztech.spark.milvus.writer

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import zilliztech.spark.milvus.MilvusOptions

case class MilvusBatchWriter(milvusOptions: MilvusOptions) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    MilvusDataWriterFactory(milvusOptions)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
