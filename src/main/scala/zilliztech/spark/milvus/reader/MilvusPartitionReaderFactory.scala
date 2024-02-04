package zilliztech.spark.milvus.reader

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import zilliztech.spark.milvus.MilvusOptions

case class MilvusPartitionReaderFactory(sparkSession: SparkSession,
                                   milvusOptions: MilvusOptions,
                                   schema: StructType)
  extends PartitionReaderFactory {
  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
//    val partition = inputPartition.asInstanceOf[MilvusPartition].partition
    new MilvusPartitionReader(sparkSession, milvusOptions, schema)
  }
}