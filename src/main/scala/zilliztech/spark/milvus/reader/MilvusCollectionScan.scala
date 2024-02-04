package zilliztech.spark.milvus.reader

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import zilliztech.spark.milvus.MilvusOptions

case class MilvusCollectionScan(sparkSession: SparkSession,
                                hadoopConf: Configuration,
                                dataSchema: StructType,
                                options: CaseInsensitiveStringMap,
                                partitionFilters: Seq[Expression] = Seq.empty,
                                dataFilters: Seq[Expression] = Seq.empty) extends Scan
  with Batch {

  override def readSchema(): StructType = dataSchema

  override def planInputPartitions(): Array[InputPartition] = {
    Array(MilvusPartition("_default"))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    val milvusOptions = new MilvusOptions(options)
    MilvusPartitionReaderFactory(sparkSession, milvusOptions, dataSchema)
  }
}
