package com.zilliz.spark.connector.read

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.spark.connector.table.MilvusTable

/** The segments of a fixed snapshot, as the tasks that read them.
  *
  * A vector search and an index build both take a snapshot and work segment by
  * segment, and both get their tasks from the one planner a scan uses, so a
  * partition means the same thing wherever it is opened
  * (docs/design/architecture/vector-search.html sections 2.1 and 2.7).
  */
private[connector] object SnapshotPartitions {

  def of(
      table: MilvusTable,
      options: CaseInsensitiveStringMap
  ): Seq[MilvusInputPartition] =
    table
      .newScanBuilder(options)
      .build()
      .toBatch
      .planInputPartitions()
      .toSeq
      .map(_.asInstanceOf[MilvusInputPartition])
}
