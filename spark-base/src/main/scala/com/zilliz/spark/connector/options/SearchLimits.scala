package com.zilliz.spark.connector.options

/** The three byte limits a vector search is planned against.
  *
  * They bound three different things: how large a query set may be before it
  * stops travelling in a broadcast variable and travels with the shuffle
  * instead, how much of it one task answers at a time, and how many bytes of
  * vectors one executor keeps while its tasks answer them
  * (docs/design/architecture/vector-search.html section 1.1). The last is None
  * unless the call set it: the default is derived from the executor's memory
  * where the search is planned (`SearchResources.vectorsBudget`).
  */
final case class SearchLimits(
    queriesMaxBytes: Long,
    groupMaxBytes: Long,
    vectorsMaxBytes: Option[Long]
) {
  require(queriesMaxBytes > 0, "queriesMaxBytes must be positive")
  require(groupMaxBytes > 0, "groupMaxBytes must be positive")
  require(vectorsMaxBytes.forall(_ > 0), "vectorsMaxBytes must be positive")
}

object SearchLimits {

  val DefaultQueriesMaxBytes: Long = 1024L * 1024L * 1024L
  val DefaultGroupMaxBytes: Long = 512L * 1024L * 1024L

  val Default: SearchLimits = SearchLimits(
    DefaultQueriesMaxBytes,
    DefaultGroupMaxBytes,
    None
  )

  def from(options: Map[String, String]): SearchLimits = {
    val lower = options.map { case (key, value) =>
      key.toLowerCase(java.util.Locale.ROOT) -> value
    }
    def get(key: String): Option[String] =
      lower
        .get(key.toLowerCase(java.util.Locale.ROOT))
        .map(_.trim)
        .filter(
          _.nonEmpty
        )
    SearchLimits(
      OptionParsing.positiveLong(
        get,
        MilvusOption.SearchQueriesMaxBytes,
        DefaultQueriesMaxBytes
      ),
      OptionParsing.positiveLong(
        get,
        MilvusOption.SearchGroupMaxBytes,
        DefaultGroupMaxBytes
      ),
      get(MilvusOption.SearchVectorsMaxBytes).map(_ =>
        OptionParsing.positiveLong(get, MilvusOption.SearchVectorsMaxBytes, 1L)
      )
    )
  }
}
