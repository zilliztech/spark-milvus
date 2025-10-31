package com.zilliz.spark.connector.read

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.MilvusS3Option

class MilvusPartitionReaderFactory(
    schema: StructType,
    options: CaseInsensitiveStringMap,
    pushedFilters: Array[Filter] = Array.empty[Filter]
) extends PartitionReaderFactory {

  private val readerOptions = MilvusS3Option(options)

  override def createReader(
      partition: InputPartition
  ): PartitionReader[InternalRow] = {
    val milvusPartition = partition.asInstanceOf[MilvusInputPartition]
    // Create the data reader with the file map, schema, and options
    new MilvusPartitionReader(
      schema,
      milvusPartition.fieldFiles,
      milvusPartition.partition,
      readerOptions,
      pushedFilters
    )
  }
}
