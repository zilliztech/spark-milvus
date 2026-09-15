package com.zilliz.spark.connector.read

/** Driver-side planning, held here until `core.read.plan` exists.
  *
  * `MilvusScan` holds the `core.snapshot.Snapshot` the table was built from;
  * `DeletePlanning` resolves its delete plans and `SnapshotPartitions.build`
  * turns it into input partitions. Where the snapshot comes from is no longer
  * decided here: `spark.options.SnapshotSources` picks the `SnapshotSource` in
  * `getTable`.
  *
  * Interim on purpose: everything here depends on Spark types, which layer 2
  * forbids. `SnapshotPartitions.build` becomes `core.read.plan.Partitioner` and
  * `DeletePlanning` moves to the executor with `DeleteSource.Files`; the
  * package ends when both are gone. It claims no capability of its own; the ids
  * stay with `read`.
  */
package object plan
