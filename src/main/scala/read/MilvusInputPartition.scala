package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.InputPartition
import com.zilliz.spark.connector.MilvusOption

// Storage V2 InputPartition - requires Milvus 2.6+
case class MilvusStorageV2InputPartition(
    manifestJson: String,
    milvusSchemaBytes: Array[Byte],  // Serialized protobuf
    partitionName: String,
    milvusOption: MilvusOption,
    topK: Option[Int] = None,
    queryVector: Option[Array[Float]] = None,
    metricType: Option[String] = None,
    vectorColumn: Option[String] = None,
    segmentID: Long = -1L
) extends InputPartition
