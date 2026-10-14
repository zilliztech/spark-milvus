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

import com.zilliz.milvus.storage.index.{EngineRange, RankingFunction}
import com.zilliz.milvus.storage.schema.MetricType
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

  /** Driver-side result assembly. Task closures are built from copied values by
    * baseValues and joiners; they never capture this join or its searcher.
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
      val keptRows = rows.keptRows
      val counts = rows.counts
      val dimension = rows.dimension
      val payload = spark.sparkContext.broadcast(keptRows)
      val newJoiner = joiners
      val connector: RDD[InternalRow] =
        if (counts.searched == 0L) {
          post(counts)
          if (!outer) spark.sparkContext.emptyRDD[InternalRow]
          else
            spark.sparkContext
              .parallelize(
                keptRows.indices,
                math.max(1, math.min(keptRows.length, 64))
              )
              .mapPartitions { numbers =>
                val joiner = newJoiner()
                numbers.map(number =>
                  joiner(payload.value(number), None).copy()
                )
              }
        } else {
          val hits = searcher.search(
            rows.packed(searcher.layoutAt(dimension)),
            dimension,
            if (outer) Some(Array.tabulate(keptRows.length)(_.toLong))
            else None
          )
          post(counts)
          val baseOf = baseValues
          hits.rows.mapPartitions { found =>
            val joiner = newJoiner()
            found.map(hit =>
              joiner(payload.value(hit.getLong(0).toInt), Some(baseOf(hit)))
            )
          }
        }
      connector
    }

    /** Rows kept and numbered on the executors, joined to their hits after the
      * last stage, so that a query side computed again in another order cannot
      * pair a hit with the wrong row.
      */
    def numbered(rows: NearestByQueryRows.Distributed): RDD[InternalRow] = {
      val counts = rows.counts
      val dimension = rows.dimension
      val numbered = rows.number()
      val queries = rows.vectors(spark, numbered)
      val hits =
        if (counts.searched == 0L) None
        else
          Some(
            searcher.search(
              queries,
              dimension,
              None
            )
          )
      post(counts)
      val baseOf = baseValues
      val hitsByQuery = hits match {
        case Some(found) => found.rows.map(hit => hit.getLong(0) -> baseOf(hit))
        case None =>
          spark.sparkContext.emptyRDD[(Long, InternalRow)]
      }
      // The rows Spark joins are not this side's to keep or drop.
      val byNumber = rows.kept(numbered)
      val paired: RDD[(InternalRow, Option[InternalRow])] =
        if (outer)
          byNumber.leftOuterJoin(hitsByQuery).values
        else
          byNumber.join(hitsByQuery).values.map { case (row, hit) =>
            row -> Some(hit)
          }
      val newJoiner = joiners
      val searched: RDD[InternalRow] = paired.mapPartitions { pairs =>
        val joiner = newJoiner()
        pairs.map { case (row, hit) => joiner(row, hit) }
      }
      searched
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

    /** A hit's base values, NULL where the hit is. */
    private def baseValues: InternalRow => InternalRow = {
      val positions = searcher.positions.toArray
      val types = searcher.baseTypes.toArray
      val columns = width
      hit => {
        val values = new GenericInternalRow(columns)
        var i = 0
        while (i < columns) {
          values.update(i, hit.get(positions(i), types(i)))
          i += 1
        }
        values
      }
    }

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
