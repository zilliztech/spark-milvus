package zilliztech.spark.milvus.reader

import org.apache.spark.sql.connector.read.InputPartition

case class MilvusPartition(partition: String) extends InputPartition {
}
