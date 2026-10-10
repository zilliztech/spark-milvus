package com.zilliz.spark.connector.metrics

import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.SparkContext

/** What a vector search counted.
  *
  * A search runs as RDD stages rather than as a DataSource V2 scan, so its
  * numbers do not reach Spark through the `CustomMetric` classes above. The
  * nearest-by join that runs it counts into the SQL metrics of its plan node,
  * which the SQL tab shows on that node and the stage page lists under the
  * names below (docs/design/architecture/dataframe-api.html section 4).
  * Knowhere computes in its own thread pool, which Spark's task CPU time does
  * not cover; `milvus.search.knowhere.nanos` is where that time shows up
  * (docs/design/architecture/vector-search.html section 1.3).
  */
final class SearchMetrics private (
    val segmentSearches: SearchMetrics.Counter,
    val readBytes: SearchMetrics.Counter,
    val readNanos: SearchMetrics.Counter,
    val indexBytes: SearchMetrics.Counter,
    val indexLoadNanos: SearchMetrics.Counter,
    val bitmapNanos: SearchMetrics.Counter,
    val knowhereCalls: SearchMetrics.Counter,
    val knowhereNanos: SearchMetrics.Counter,
    val comparedPairs: SearchMetrics.Counter,
    val candidates: SearchMetrics.Counter,
    val takeRows: SearchMetrics.Counter,
    val takeNanos: SearchMetrics.Counter
) extends Serializable {

  /** Every counter with the name it carries, for logging and for tests. */
  def all: Seq[(String, SearchMetrics.Counter)] = Seq(
    SearchMetrics.SegmentSearches -> segmentSearches,
    SearchMetrics.ReadBytes -> readBytes,
    SearchMetrics.ReadNanos -> readNanos,
    SearchMetrics.IndexBytes -> indexBytes,
    SearchMetrics.IndexLoadNanos -> indexLoadNanos,
    SearchMetrics.BitmapNanos -> bitmapNanos,
    SearchMetrics.KnowhereCalls -> knowhereCalls,
    SearchMetrics.KnowhereNanos -> knowhereNanos,
    SearchMetrics.ComparedPairs -> comparedPairs,
    SearchMetrics.Candidates -> candidates,
    SearchMetrics.TakeRows -> takeRows,
    SearchMetrics.TakeNanos -> takeNanos
  )

  def summary: String =
    all.map { case (name, value) => s"$name=${value.value}" }.mkString(", ")
}

object SearchMetrics {

  /** Not the segments a search covers: a segment is searched once per query
    * group, so this is the pairs of the two, and the total is their product.
    */
  val SegmentSearches = "milvus.search.segment.searches"
  val ReadBytes = "milvus.search.read.bytes"
  val ReadNanos = "milvus.search.read.nanos"
  val IndexBytes = "milvus.search.index.bytes"
  val IndexLoadNanos = "milvus.search.index.load.nanos"
  val BitmapNanos = "milvus.search.bitmap.nanos"
  val KnowhereCalls = "milvus.search.knowhere.calls"
  val KnowhereNanos = "milvus.search.knowhere.nanos"

  /** Query and base-vector pairs an exact scan measured a distance for. The
    * planner knows the total, so this one has a denominator: an index probe
    * cannot count them and leaves it at zero.
    */
  val ComparedPairs = "milvus.search.compared.pairs"
  val Candidates = "milvus.search.candidates"
  val TakeRows = "milvus.search.take.rows"
  val TakeNanos = "milvus.search.take.nanos"

  /** One number a search's tasks add to, under the name it was registered by.
    */
  sealed trait Counter extends Serializable {
    def add(value: Long): Unit
    def value: Long
    def name: Option[String]
  }

  private final case class Measured(metric: SQLMetric) extends Counter {
    override def add(value: Long): Unit = metric.add(value)
    override def value: Long = metric.value
    override def name: Option[String] = metric.name
  }

  /** The SQL metrics of a plan node that runs a search, by name: bytes as sizes
    * and nanoseconds as durations, which the SQL tab formats. A size or a
    * duration nothing was added to reports -1 for the task, Spark's mark of no
    * value.
    */
  def sqlMetrics(context: SparkContext): Map[String, SQLMetric] = Map(
    SegmentSearches -> SQLMetrics.createMetric(context, SegmentSearches),
    ReadBytes -> SQLMetrics.createSizeMetric(context, ReadBytes),
    ReadNanos -> SQLMetrics.createNanoTimingMetric(context, ReadNanos),
    IndexBytes -> SQLMetrics.createSizeMetric(context, IndexBytes),
    IndexLoadNanos -> SQLMetrics
      .createNanoTimingMetric(context, IndexLoadNanos),
    BitmapNanos -> SQLMetrics.createNanoTimingMetric(context, BitmapNanos),
    KnowhereCalls -> SQLMetrics.createMetric(context, KnowhereCalls),
    KnowhereNanos -> SQLMetrics.createNanoTimingMetric(context, KnowhereNanos),
    ComparedPairs -> SQLMetrics.createMetric(context, ComparedPairs),
    Candidates -> SQLMetrics.createMetric(context, Candidates),
    TakeRows -> SQLMetrics.createMetric(context, TakeRows),
    TakeNanos -> SQLMetrics.createNanoTimingMetric(context, TakeNanos)
  )

  /** A search counted in the SQL metrics [[sqlMetrics]] made. */
  def of(metrics: Map[String, SQLMetric]): SearchMetrics = {
    def counter(name: String): Counter = Measured(
      metrics.getOrElse(
        name,
        throw new IllegalArgumentException(s"No SQL metric named $name")
      )
    )
    new SearchMetrics(
      counter(SegmentSearches),
      counter(ReadBytes),
      counter(ReadNanos),
      counter(IndexBytes),
      counter(IndexLoadNanos),
      counter(BitmapNanos),
      counter(KnowhereCalls),
      counter(KnowhereNanos),
      counter(ComparedPairs),
      counter(Candidates),
      counter(TakeRows),
      counter(TakeNanos)
    )
  }
}
