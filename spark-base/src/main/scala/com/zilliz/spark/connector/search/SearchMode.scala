package com.zilliz.spark.connector.search

/** How a search finds each query's candidates: `Exact` scans the segments,
  * while `PreferIndex` uses each segment's usable persisted index pinned by the
  * snapshot and scans a segment exactly when no usable index is available
  * (docs/design/projects/spark-milvus/architecture/vector-search.html section
  * 1.1). An EXACT nearest-by join searches in `Exact` mode and an APPROX one in
  * `PreferIndex` mode. `name` is what `toString` gives.
  */
private[search] sealed abstract class SearchMode(val name: String)
    extends Product
    with Serializable {
  override def toString: String = name
}

private[search] object SearchMode {
  case object PreferIndex extends SearchMode("index")
  case object Exact extends SearchMode("exact")
}
