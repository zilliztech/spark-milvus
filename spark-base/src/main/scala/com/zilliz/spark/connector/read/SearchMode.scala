package com.zilliz.spark.connector.read

/** How a search finds each query's candidates: `Exact` computes every distance
  * in every segment, `Index` searches the persisted index the snapshot pinned
  * (docs/design/architecture/vector-search.html section 1.1). An EXACT
  * nearest-by join searches in `Exact` mode and an APPROX one in `Index` mode.
  * `name` is what `toString` gives.
  */
private[read] sealed abstract class SearchMode(val name: String)
    extends Product
    with Serializable {
  override def toString: String = name
}

private[read] object SearchMode {
  case object Index extends SearchMode("index")
  case object Exact extends SearchMode("exact")
}
