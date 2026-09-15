package com.zilliz.spark.connector.read.plan

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.options.{
  MilvusOption,
  StorageOptions,
  VectorSearch
}

/** What planning needs from the scan: the options and the `MilvusOption` parsed
  * from them once.
  */
private[read] final class ScanContext(
    val options: CaseInsensitiveStringMap,
    val milvusOption: MilvusOption
) {
  val vectorSearch: Option[VectorSearch] = milvusOption.vectorSearch

  /** Hadoop configuration for objects under `path`, from the connector's `fs.*`
    * options plus per-bucket S3A settings.
    */
  def hadoopConf(path: String): Configuration =
    StorageOptions.buildHadoopConfForOptions(milvusOption.options, path)
}
