package com.zilliz.spark.connector

/** The one place the connector's counters become Spark metrics: `SumMetric`,
  * `MaxBytesMetric` and `TaskMetric` are the DataSource V2 `CustomMetric` /
  * `CustomTaskMetric` shapes, `ScanMetrics` lists what a scan declares and its
  * readers report, `WriteMetricsReport` the same for a write. The numbers come
  * from `core.read.exec.ReadMetrics` and `core.write.exec.WriteMetrics`;
  * nothing is measured here. Design: docs/design/architecture/storage-io.html
  * section 5.
  *
  * Capabilities: G5 (see docs/design/capabilities.md).
  */
package object metrics
