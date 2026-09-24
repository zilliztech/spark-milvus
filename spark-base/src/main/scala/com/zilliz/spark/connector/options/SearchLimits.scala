package com.zilliz.spark.connector.options

/** The limits a vector search is planned against: three byte limits and one
  * switch.
  *
  * The byte limits bound three different things: how large a query set may be
  * before it stops travelling in a broadcast variable and travels with the
  * shuffle instead, how much of it one task answers at a time, and how many
  * bytes of segment data -- vectors in exact mode, indexes in index mode -- one
  * executor keeps off the heap while its tasks answer them
  * (docs/design/architecture/vector-search.html section 1.1). The last is None
  * unless the call set it: the default is derived from the executor's memory
  * where the search is planned (`SearchResources.segmentBudget`). The switch,
  * `queriesDirect`, lets a query set that is a plain Parquet scan be read by
  * the search tasks themselves instead of collected and broadcast
  * (`milvus.search.queries.direct`, default true).
  */
final case class SearchLimits(
    queriesMaxBytes: Long,
    groupMaxBytes: Long,
    segmentsMaxBytes: Option[Long],
    queriesDirect: Boolean = SearchLimits.DefaultQueriesDirect
) {
  require(queriesMaxBytes > 0, "queriesMaxBytes must be positive")
  require(groupMaxBytes > 0, "groupMaxBytes must be positive")
  require(segmentsMaxBytes.forall(_ > 0), "segmentsMaxBytes must be positive")
}

object SearchLimits {

  val DefaultQueriesMaxBytes: Long = 1024L * 1024L * 1024L
  val DefaultGroupMaxBytes: Long = 512L * 1024L * 1024L

  /** A query frame that is a plain scan of Parquet files is read by the search
    * tasks themselves rather than collected and broadcast.
    */
  val DefaultQueriesDirect: Boolean = true

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
      get(MilvusOption.SearchSegmentsMaxBytes).map(_ =>
        OptionParsing.positiveLong(get, MilvusOption.SearchSegmentsMaxBytes, 1L)
      ),
      OptionParsing.boolean(
        get,
        MilvusOption.SearchQueriesDirect,
        DefaultQueriesDirect
      )
    )
  }
}
