package com.zilliz.spark.connector

/** ScanBuilder, Scan, Batch, InputPartition, the columnar PartitionReader and
  * the ColumnVector implementations.
  *
  * Planning lives here until `core.read.plan` exists: `MilvusScan` picks one
  * `PartitionPlanner` by `ReadMode` (ClientSnapshotPlanner and
  * LegacyClientPlanner for a live service, OptionSnapshotPlanner for snapshot
  * options, BackupPlanner for a milvus-backup export); each produces a segment
  * list that `SnapshotPartitions.build` turns into input partitions with the
  * delete plans from `DeletePlanning`. `ClientReadSnapshot` owns the snapshot a
  * client-mode read creates on the service.
  *
  * Capabilities: R4, R5, R11, R12, R13, R16, R18 (see docs/design/capabilities.md).
  */
package object scan
