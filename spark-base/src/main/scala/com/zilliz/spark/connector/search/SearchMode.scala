package com.zilliz.spark.connector.search

/** How a search finds each query's candidates: `Exact` scans the segments,
  * while `Index` searches each segment's usable persisted index pinned by the
  * snapshot and scans a segment exactly when no usable index is available
  * (docs/design/architecture/vector-search.html section 1.1). An EXACT
  * nearest-by join searches in `Exact` mode and an APPROX one in `Index` mode.
  * `name` is what `toString` gives.
  */
private[search] sealed abstract class SearchMode(val name: String)
    extends Product
    with Serializable {
  override def toString: String = name
}

private[search] object SearchMode {
  case object Index extends SearchMode("index")
  case object Exact extends SearchMode("exact")
}
