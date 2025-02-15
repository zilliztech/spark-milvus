package zilliztech.spark.milvus.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, DataWriterFactory}
import zilliztech.spark.milvus.MilvusOptions
import org.apache.spark.sql.types.StructType

case class MilvusDataWriterFactory(milvusOptions: MilvusOptions, schema: StructType)
  extends DataWriterFactory
    with Serializable {
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    MilvusDataWriter(partitionId, taskId, milvusOptions, schema)
  }
}
