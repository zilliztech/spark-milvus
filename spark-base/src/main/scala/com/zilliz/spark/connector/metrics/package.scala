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
  * stages rather than as a scan: the search reports segment, read, index,
  * bitmap, Knowhere, candidate and take numbers into it
  * (docs/design/architecture/vector-search.html section 1.3). The nearest-by
  * join that runs the search counts into SQL metrics of its node, which
  * `NearestByMetrics` declares together with that node's output rows and the
  * driver's counts of query rows and segments
  * (docs/design/architecture/dataframe-api.html section 4).
  *
  * Capabilities: G5 (see docs/design/capabilities.md).
  */
package object metrics
