package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.InputPartition
import com.zilliz.spark.connector.MilvusOption

// Storage V2 InputPartition - requires Milvus 2.6+
case class MilvusStorageV2InputPartition(
    manifestPath: String,  // Path to manifest in S3/MinIO
    milvusSchemaBytes: Array[Byte],  // Serialized protobuf
    partitionName: String,
    milvusOption: MilvusOption,
    topK: Option[Int] = None,
    queryVector: Option[Array[Float]] = None,
    metricType: Option[String] = None,
    vectorColumn: Option[String] = None,
    segmentID: Long = -1L,
    readVersion: Long = -1L  // -1 = LATEST, >0 = specific manifest version from snapshot
) extends InputPartition
