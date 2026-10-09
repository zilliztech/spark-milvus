package com.zilliz.spark.connector

/** The one place the connector's counters become Spark metrics: `SumMetric`,
  * `MaxBytesMetric` and `TaskMetric` are the DataSource V2 `CustomMetric` /
  * `CustomTaskMetric` shapes, one class per declared metric (Spark re-creates a
  * metric from its class name), `ScanMetrics` lists what a scan declares and
  * its readers report, `WriteMetricsReport` the same for a write. The numbers
  * come from `core.read.exec.ReadMetrics` and `core.write.exec.WriteMetrics`;
  * nothing is measured here. Design: docs/design/architecture/storage-io.html
  * section 5.
  *
  * `SearchMetrics` is the same idea for a vector search, which runs as RDD
  * stages rather than as a scan: its counters are named accumulators the stage
  * page shows, and the search reports segment, read, index, bitmap, Knowhere,
  * candidate and take numbers into them
  * (docs/design/architecture/vector-search.html section 1.3).
  *
  * Capabilities: G5 (see docs/design/capabilities.md).
  */
package object metrics
