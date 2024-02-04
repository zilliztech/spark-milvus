package zilliztech.spark.milvus.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import zilliztech.spark.milvus.{MilvusOptions, MilvusUtils}

/**
 * Read data for each milvus partition
 */
class MilvusPartitionReader(spark: SparkSession, milvusOptions: MilvusOptions, schema: StructType) extends PartitionReader[InternalRow] {

  override def next(): Boolean = false

  override def get(): InternalRow = null

  override def close(): Unit = {}

//  var df: DataFrame
//  var rowNum: Long
//  var readedNum: Long
//
//  def pre(): Unit = {
////    this.df = MilvusUtils.readMilvusCollection(spark, milvusOptions)
////    rowNum = df.count()
//    readedNum = 0
//  }
}