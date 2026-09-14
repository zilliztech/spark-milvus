package com.zilliz.spark.connector.read

/** Driver-side planning, held here until `core.read.plan` exists.
  *
  * `MilvusScan` picks one `PartitionPlanner` by `ReadMode`: ClientSnapshotPlanner
  * and LegacyClientPlanner for a live service, OptionSnapshotPlanner for
  * snapshot options, BackupPlanner for a milvus-backup export. Each produces a
  * segment list that `SnapshotPartitions.build` turns into input partitions
  * with the delete plans from `DeletePlanning`; `ClientReadSnapshot` owns the
  * snapshot a client-mode read creates on the service.
  *
  * Interim on purpose: everything here depends on Spark types, which layer 2
  * forbids. Work items 03, 04 and 05 replace one planner each with a
  * `SnapshotSource`, and `SnapshotPartitions.build` becomes `core.read.plan`,
  * the package this one is named after. It ends when the last planner is
  * gone. It claims no capability of its own; the ids stay with `read`.
  */
package object plan
