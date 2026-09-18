package com.zilliz.spark.connector.metrics

import org.apache.spark.util.LongAccumulator
import org.apache.spark.SparkContext

/** What a vector search counted, as named accumulators.
  *
  * A search runs as RDD stages rather than as a DataSource V2 scan, so its
  * numbers reach the stage page through accumulators instead of the
  * `CustomMetric` classes above. Knowhere computes in its own thread pool,
  * which Spark's task CPU time does not cover; `milvus.search.knowhere.nanos`
  * is where that time shows up (docs/design/architecture/vector-search.html
  * section 1.3).
  */
final class SearchMetrics private (
    val segments: LongAccumulator,
    val readBytes: LongAccumulator,
    val readNanos: LongAccumulator,
    val indexBytes: LongAccumulator,
    val indexLoadNanos: LongAccumulator,
    val bitmapNanos: LongAccumulator,
    val knowhereCalls: LongAccumulator,
    val knowhereNanos: LongAccumulator,
    val candidates: LongAccumulator,
    val takeRows: LongAccumulator,
    val takeNanos: LongAccumulator
) extends Serializable {

  /** Every accumulator with the name it carries, for logging and for tests. */
  def all: Seq[(String, LongAccumulator)] = Seq(
    SearchMetrics.Segments -> segments,
    SearchMetrics.ReadBytes -> readBytes,
    SearchMetrics.ReadNanos -> readNanos,
    SearchMetrics.IndexBytes -> indexBytes,
    SearchMetrics.IndexLoadNanos -> indexLoadNanos,
    SearchMetrics.BitmapNanos -> bitmapNanos,
    SearchMetrics.KnowhereCalls -> knowhereCalls,
    SearchMetrics.KnowhereNanos -> knowhereNanos,
    SearchMetrics.Candidates -> candidates,
    SearchMetrics.TakeRows -> takeRows,
    SearchMetrics.TakeNanos -> takeNanos
  )

  def summary: String =
    all.map { case (name, value) => s"$name=${value.value}" }.mkString(", ")
}

object SearchMetrics {

  val Segments = "milvus.search.segments"
  val ReadBytes = "milvus.search.read.bytes"
  val ReadNanos = "milvus.search.read.nanos"
  val IndexBytes = "milvus.search.index.bytes"
  val IndexLoadNanos = "milvus.search.index.load.nanos"
  val BitmapNanos = "milvus.search.bitmap.nanos"
  val KnowhereCalls = "milvus.search.knowhere.calls"
  val KnowhereNanos = "milvus.search.knowhere.nanos"
  val Candidates = "milvus.search.candidates"
  val TakeRows = "milvus.search.take.rows"
  val TakeNanos = "milvus.search.take.nanos"

  /** One set per search: a second search registers its own, so the numbers
    * belong to one job rather than to the session.
    */
  def create(context: SparkContext): SearchMetrics = new SearchMetrics(
    context.longAccumulator(Segments),
    context.longAccumulator(ReadBytes),
    context.longAccumulator(ReadNanos),
    context.longAccumulator(IndexBytes),
    context.longAccumulator(IndexLoadNanos),
    context.longAccumulator(BitmapNanos),
    context.longAccumulator(KnowhereCalls),
    context.longAccumulator(KnowhereNanos),
    context.longAccumulator(Candidates),
    context.longAccumulator(TakeRows),
    context.longAccumulator(TakeNanos)
  )
}
