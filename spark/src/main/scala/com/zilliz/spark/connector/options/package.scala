package com.zilliz.spark.connector

/** Public read, write and storage option parsing. MilvusOption describes
  * snapshot selection, projections, scalar filters, read limits, read modes,
  * the row identity and the write operation. StorageOptions and
  * HadoopStorageKeys resolve per-input storage settings. OptionParsing provides
  * strict shared value parsing; TaskResources describes executor resources and
  * stage-level task core allocation.
  */
package object options
