package com.zilliz.spark.connector.search

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{
  Expression,
  GenericInternalRow,
  JoinedRow,
  UnsafeProjection
}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.schema.MetricType
import com.zilliz.milvus.storage.vector.{EngineRange, RankingFunction}
import com.zilliz.spark.connector.metrics.NearestByMetrics

/** Coordinates a nearest-by join: adapts its input, prepares its query rows,
  * searches the eligible rows and attaches the hits by query number. Rows the
  * engine cannot search run through Spark's join and meet the searched results
  * here. A LEFT OUTER query with no hit keeps its row with NULL base columns.
  *
  * Input behavior lives in [[NearestByInput]], classification and stable query
  * numbering in [[NearestByQueryRows]]. Result assembly stays here: local query
  * rows attach through a broadcast, distributed rows through a join.
  */
private[connector] object NearestBySearch {

  /** @param queryTypes
    *   the query side's column types, in its output order
    * @param queryVector
    *   bound to the query rows
    * @param outputTypes
    *   the join's output: the query columns, then the base columns
    * @param function
    *   what the base rows outside [[EngineRange]] are scored by
    * @param bySpark
    *   Spark's own execution of the join for the query rows it is given, in the
    *   join's output columns; called on the driver
    * @param metrics
    *   the SQL metrics of the node that runs the join ([[NearestByMetrics]])
    */
  def run(
      spark: SparkSession,
      queries: NearestByQueryRows.Source,
      queryTypes: Seq[DataType],
      queryVector: Expression,
      input: NearestByInput,
      outer: Boolean,
      approx: Boolean,
      k: Int,
      metric: MetricType,
      outputTypes: Seq[DataType],
      function: RankingFunction,
      bySpark: RDD[InternalRow] => RDD[InternalRow],
      metrics: Map[String, SQLMetric]
  ): RDD[InternalRow] = {
    val searcher = NearestByInput.searcher(
      spark,
      input,
      approx,
      k,
      metric,
      function,
      metrics
    )
    val prepared = NearestByQueryRows.prepare(
      queries,
      queryTypes.toArray,
      queryVector,
      searcher
    )
    val join = new Join(
      spark,
      searcher,
      outer,
      outputTypes.toArray,
      outputTypes.size - queryTypes.size,
      metrics
    )
    val joinedBySpark = prepared.sparkRows(spark).map(bySpark)
    val searched = prepared match {
      case local: NearestByQueryRows.Local => join.held(local)
      case distributed: NearestByQueryRows.Distributed =>
        join.numbered(distributed)
    }
    val result = joinedBySpark.fold(searched)(searched.union(_))
    val outputRows = metrics(NearestByMetrics.OutputRows)
    result.mapPartitions { rows =>
      rows.map { row =>
        outputRows.add(1L)
        row
      }
    }
  }

  /** Driver-side result assembly. Task closures use copied values and joiners;
    * they never capture this join or its searcher.
    */
  private final class Join(
      spark: SparkSession,
      searcher: NearestByInput.Searcher,
      outer: Boolean,
      output: Array[DataType],
      width: Int,
      metrics: Map[String, SQLMetric]
  ) {

    /** Attaches the query rows held on the driver to their hits by a broadcast.
      */
    def held(rows: NearestByQueryRows.Local): RDD[InternalRow] = {
      val counts = rows.counts
      if (counts.searched == 0L) {
        return missing(counts) {
          val keptRows = rows.keptRows
          spark.sparkContext.parallelize(
            keptRows.toIndexedSeq,
            math.max(1, math.min(keptRows.length, 64))
          )
        }
      }
      val keptRows = rows.keptRows
      val dimension = rows.dimension
      val payload = spark.sparkContext.broadcast(keptRows)
      val hits = searcher.search(
        rows.packed(searcher.layoutAt(dimension)),
        dimension,
        if (outer) Some(Array.tabulate(keptRows.length)(_.toLong))
        else None
      )
      post(counts)
      val newJoiner = joiners
      hits.mapPartitions { found =>
        val joiner = newJoiner()
        found.map { case (query, base) =>
          joiner(payload.value(query.toInt), Some(base))
        }
      }
    }

    /** Rows kept and numbered on the executors, joined to their hits after the
      * last stage, so that a query side computed again in another order cannot
      * pair a hit with the wrong row.
      */
    def numbered(rows: NearestByQueryRows.Distributed): RDD[InternalRow] = {
      val counts = rows.counts
      if (counts.searched == 0L) return missing(counts)(rows.keptRows)
      val numbered = rows.number()
      val hits = searcher.search(rows.vectors(numbered), rows.dimension, None)
      post(counts)
      // The rows Spark joins are not this side's to keep or drop.
      val byNumber = rows.kept(numbered)
      val paired: RDD[(InternalRow, Option[InternalRow])] =
        if (outer)
          byNumber.leftOuterJoin(hits).values
        else
          byNumber.join(hits).values.map { case (row, hit) =>
            row -> Some(hit)
          }
      val newJoiner = joiners
      paired.mapPartitions { pairs =>
        val joiner = newJoiner()
        pairs.map { case (row, hit) => joiner(row, hit) }
      }
    }

    /** With no searched queries, INNER needs no rows and OUTER only NULLs.
      * Delay obtaining the rows so INNER also skips copying local rows.
      */
    private def missing(counts: NearestByQueryRows.Counts)(
        rows: => RDD[InternalRow]
    ): RDD[InternalRow] = {
      post(counts)
      if (!outer || counts.unranked == 0L)
        spark.sparkContext.emptyRDD[InternalRow]
      else {
        val newJoiner = joiners
        rows.mapPartitions { queries =>
          val joiner = newJoiner()
          queries.map(row => joiner(row, None))
        }
      }
    }

    // What the driver settled, on the node.
    private def post(counts: NearestByQueryRows.Counts): Unit =
      NearestByMetrics.postDriverValues(
        spark.sparkContext,
        metrics,
        Map(
          NearestByMetrics.SearchedQueries -> counts.searched,
          NearestByMetrics.SparkQueries -> counts.bySpark,
          NearestByMetrics.UnrankedQueries -> counts.unranked
        ) ++ searcher.segments(counts.searched)
      )

    /** What makes a [[Joiner]] in a task. */
    private def joiners: () => Joiner = {
      val types = output
      val columns = width
      () => new Joiner(types, columns)
    }
  }

  /** A query row joined to its hit's base values, or to NULLs where it has no
    * hit, as a row of the join's output. Each task makes its own: a projection
    * is not shared.
    */
  private final class Joiner(output: Array[DataType], width: Int) {
    private val project = UnsafeProjection.create(output)
    private val joined = new JoinedRow()
    private val absent = new GenericInternalRow(width)

    def apply(row: InternalRow, base: Option[InternalRow]): InternalRow =
      project(joined(row, base.getOrElse(absent)))
  }

}
