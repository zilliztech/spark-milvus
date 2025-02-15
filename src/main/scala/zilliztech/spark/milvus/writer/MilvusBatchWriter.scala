package zilliztech.spark.milvus.writer

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import zilliztech.spark.milvus.MilvusOptions

case class MilvusBatchWriter(milvusOptions: MilvusOptions, schema: StructType) extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory = {
    MilvusDataWriterFactory(milvusOptions, schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}
