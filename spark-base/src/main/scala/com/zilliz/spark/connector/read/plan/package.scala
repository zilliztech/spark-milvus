package com.zilliz.spark.connector.read

/** Driver-side planning, held here until `core.read.plan` exists.
  *
  * `MilvusScan` picks one `PartitionPlanner` by `ReadMode`: ClientSnapshotPlanner
  * for a live service (the service names the collection id, the snapshot
  * comes from the snapshot directory through `core.snapshot.SnapshotCatalog`),
  * OptionSnapshotPlanner for `milvus.snapshot.path` or the 1.x option strings,
  * BackupPlanner for a milvus-backup export. Each produces a
  * `core.snapshot.Snapshot`; `SnapshotPartitions.build` turns it into input
  * partitions with the delete plans from `DeletePlanning`.
  *
  * Interim on purpose: everything here depends on Spark types, which layer 2
  * forbids. Work items 03, 04 and 05 replace one planner each with a
  * `SnapshotSource`, and `SnapshotPartitions.build` becomes `core.read.plan`,
  * the package this one is named after. It ends when the last planner is
  * gone. It claims no capability of its own; the ids stay with `read`.
  */
package object plan
