package com.zilliz.spark.connector.options

/** The limits a vector search is planned against: three byte limits and three
  * counts.
  *
  * The byte limits bound three different things: how large a query set may be
  * before it stops travelling in a broadcast variable and travels with the
  * shuffle instead, how much of it one task answers at a time, and how many
  * bytes of segment data -- vectors in exact mode, indexes in index mode -- one
  * executor keeps off the heap while its tasks answer them
  * (docs/design/projects/spark-milvus/architecture/vector-search.html section
  * 1.1). They are read options of the base a nearest-by join searches. The last
  * two are None unless the base set them, and the planner derives them where
  * the search is planned: a group from the cores and the heap a task holds
  * (`SearchResources.groupBytes`), the segment data from the executor's memory
  * (`SearchResources.segmentBudget`). The counts shape the first stage:
  * `collectThreads` is how many threads a search task checks, collects and
  * packs a group's candidates on between two Knowhere calls (0, the default, is
  * every core the task holds), `queryRanges` cuts the first stage into that
  * many query ranges per segment set (0, the default, leaves the cut to the
  * planner), and `callsInFlight` is how many segments of a held set a task
  * searches at once within a query group.
  */
final case class SearchLimits(
    queriesMaxBytes: Long,
    groupMaxBytes: Option[Long],
    segmentsMaxBytes: Option[Long],
    collectThreads: Int = SearchLimits.DefaultCollectThreads,
    queryRanges: Int = SearchLimits.DefaultQueryRanges,
    callsInFlight: Int = SearchLimits.DefaultCallsInFlight
) {
  require(callsInFlight >= 1, "callsInFlight must be at least 1")
  require(queriesMaxBytes > 0, "queriesMaxBytes must be positive")
  require(collectThreads >= 0, "collectThreads must not be negative")
  require(queryRanges >= 0, "queryRanges must not be negative")
  require(groupMaxBytes.forall(_ > 0), "groupMaxBytes must be positive")
  require(segmentsMaxBytes.forall(_ > 0), "segmentsMaxBytes must be positive")
}

object SearchLimits {

  val DefaultQueriesMaxBytes: Long = 1024L * 1024L * 1024L

  /** The least a derived group takes (`SearchResources.groupBytes`), and the
    * fixed size the option once defaulted to.
    */
  val DefaultGroupMaxBytes: Long = 512L * 1024L * 1024L

  /** Threads a search task checks, collects and packs a group's candidates on
    * between two Knowhere calls: 0 means the cores the task holds
    * (`milvus.search.collect.threads`).
    */
  val DefaultCollectThreads: Int = 0

  /** Query ranges the first stage is cut into: 0 lets the planner decide
    * (`milvus.search.query.ranges`).
    */
  val DefaultQueryRanges: Int = 0

  /** Index segments one search task searches at once within a query group
    * (`milvus.search.calls.in.flight`): with 2, the collect of one segment's
    * answer and the tail of its call overlap the next segment's search
    * (docs/design/projects/spark-milvus/enhance/2026-10-13-search-task-pipelining.html);
    * 1 is the sequential search.
    */
  val DefaultCallsInFlight: Int = 2

  val Default: SearchLimits = SearchLimits(DefaultQueriesMaxBytes, None, None)

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
      get(MilvusOption.SearchGroupMaxBytes).map(_ =>
        OptionParsing.positiveLong(get, MilvusOption.SearchGroupMaxBytes, 1L)
      ),
      get(MilvusOption.SearchSegmentsMaxBytes).map(_ =>
        OptionParsing.positiveLong(get, MilvusOption.SearchSegmentsMaxBytes, 1L)
      ),
      get(MilvusOption.SearchCollectThreads)
        .map(raw =>
          OptionParsing.nonNegativeLong(raw, MilvusOption.SearchCollectThreads)
        )
        .map(value => math.min(value, Int.MaxValue.toLong).toInt)
        .getOrElse(DefaultCollectThreads),
      get(MilvusOption.SearchQueryRanges)
        .map(raw =>
          OptionParsing.nonNegativeLong(raw, MilvusOption.SearchQueryRanges)
        )
        .map(value => math.min(value, Int.MaxValue.toLong).toInt)
        .getOrElse(DefaultQueryRanges),
      get(MilvusOption.SearchCallsInFlight)
        .map(_ =>
          OptionParsing.positiveLong(
            get,
            MilvusOption.SearchCallsInFlight,
            DefaultCallsInFlight.toLong
          )
        )
        .map(value => math.min(value, 8L).toInt)
        .getOrElse(DefaultCallsInFlight)
    )
  }
}
