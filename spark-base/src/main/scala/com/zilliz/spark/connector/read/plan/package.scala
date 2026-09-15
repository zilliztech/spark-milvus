package com.zilliz.spark.connector.read

/** Driver-side reading of delete files, held here until it moves to the
  * executor.
  *
  * `DeletePlanning` reads every delete file a snapshot names into a
  * `DeletePlan` on the driver, and `MilvusScan` hands those plans to
  * `core.read.plan.ReadPlan.of`, which ships them inside each task as
  * `DeleteSource.Materialized`. The planning itself left this package: the
  * segment list becomes tasks in core, and `MilvusScan.inputPartitions` only
  * wraps them into Spark's partitions.
  *
  * Interim on purpose: the package ends when the executor reads the delete
  * files itself from `DeleteSource.Files`, which is a change of behaviour (the
  * driver ships paths, not primary-key maps; an L0 file is read once per task)
  * and has its own work item. It claims no capability of its own; the ids stay
  * with `read`.
  */
package object plan
