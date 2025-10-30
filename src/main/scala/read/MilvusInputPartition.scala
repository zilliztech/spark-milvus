package com.zilliz.spark.connector.read

import org.apache.spark.sql.connector.read.InputPartition
import com.zilliz.spark.connector.MilvusOption

// Milvus InputPartition for segments with storage version 0
case class MilvusInputPartition(
    fieldFiles: Seq[Map[String, String]],
    partition: String = ""
) extends InputPartition

