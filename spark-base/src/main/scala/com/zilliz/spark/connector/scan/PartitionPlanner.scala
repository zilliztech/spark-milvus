package com.zilliz.spark.connector.scan

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.options.{StorageOptions, VectorSearchConfig}
import com.zilliz.spark.connector.options.MilvusOption

/** What every planner needs from the scan: the options and the `MilvusOption`
  * parsed from them once.
  */
private[scan] final class ScanContext(
    val options: CaseInsensitiveStringMap,
    val milvusOption: MilvusOption
) {
  val vectorSearchConfig: Option[VectorSearchConfig] =
    milvusOption.vectorSearchConfig

  /** Hadoop configuration for objects under `path`, from the connector's `fs.*`
    * options plus per-bucket S3A settings.
    */
  def hadoopConf(path: String): Configuration =
    StorageOptions.buildHadoopConfForOptions(milvusOption.options, path)
}

/** One planning entry point: turns the segment list of one source into Spark
  * input partitions. Four exist today (client snapshot, legacy client, option
  * snapshot, backup); `core.read.plan` is where their common tail goes.
  */
private[scan] abstract class PartitionPlanner(protected val ctx: ScanContext)
    extends Logging {
  protected def options: CaseInsensitiveStringMap = ctx.options
  protected def milvusOption: MilvusOption = ctx.milvusOption
  protected def vectorSearchConfig: Option[VectorSearchConfig] =
    ctx.vectorSearchConfig
}
