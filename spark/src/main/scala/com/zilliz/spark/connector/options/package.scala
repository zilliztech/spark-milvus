package com.zilliz.spark.connector

/** Public read, write and storage option parsing. MilvusOption describes
  * snapshot partition selection, projections, read limits, read modes, the row
  * identity and the write operation. No option filters rows by value, and the
  * removed `milvus.filter` option fails instead of being ignored.
  * StorageOptions and HadoopStorageKeys resolve per-input storage settings.
  * OptionParsing provides strict shared value parsing; TaskResources describes
  * executor resources and stage-level task core allocation.
  */
package object options
