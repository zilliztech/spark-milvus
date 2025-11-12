package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.InputPartition
import com.zilliz.spark.connector.MilvusOption

// V2 Storage InputPartition
case class MilvusStorageV2InputPartition(
    manifestJson: String,
    milvusSchemaBytes: Array[Byte],  // Serialized protobuf
    partitionName: String,
    milvusOption: MilvusOption,
    topK: Option[Int] = None,
    queryVector: Option[Array[Float]] = None,
    metricType: Option[String] = None,
    vectorColumn: Option[String] = None,
    segmentID: Long = -1L  // Add segment ID tracking for V2
) extends InputPartition

// V1 Binlog InputPartition
case class MilvusInputPartition(
    fieldFiles: Seq[Map[String, String]],
    partition: String = "",
    segmentID: Long = -1L  // Add segment ID tracking
) extends InputPartition

