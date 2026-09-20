package com.zilliz.spark.connector.read

import java.util.Locale
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode, udaf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.StorageLevel

import com.zilliz.milvus.storage.expr.PlanParser
import com.zilliz.milvus.storage.index.SearchPlan
import com.zilliz.milvus.storage.read.exec.SegmentIndexHandle
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.options.{
  MilvusOption,
  SearchLimits,
  SnapshotReference
}
import com.zilliz.spark.connector.table.MilvusTables
import io.milvus.grpc.schema.{DataType, FieldSchema}

/** The vector search entry: a query set in, every query's global top-k out.
  *
  * `mode = "exact"` computes every distance in every segment; `mode = "index"`
  * searches the persisted index the snapshot pinned, and falls back to an exact
  * scan on a segment the snapshot says has no index when `allowUnindexed` is
  * on. The result carries `query_id`, `rank`, `_score`, `_segment_id`,
  * `_row_offset` and the output columns asked for
  * (docs/design/architecture/vector-search.html section 1.1).
  */
object MilvusSearch extends Logging {

  private val Reserved =
    Set("query_id", "rank", "_score", "_segment_id", "_row_offset")

  def search(
      spark: SparkSession,
      options: Map[String, String],
      queries: DataFrame,
      vectorColumn: String,
      k: Int,
      metric: String,
      mode: String = "index",
      searchParameters: Map[String, String] = Map.empty,
      filter: Option[String] = None,
      outputColumns: Seq[String] = Seq.empty,
      allowUnindexed: Boolean = false
  ): DataFrame = {
    require(
      options != null && queries != null && outputColumns != null &&
        searchParameters != null,
      "Search arguments must not be null"
    )
    require(
      vectorColumn != null && vectorColumn.nonEmpty && metric != null,
      "A search names a vector field and a metric"
    )
    require(k > 0, s"k must be positive: $k")
    val searchMode = mode.toLowerCase(Locale.ROOT)
    require(
      searchMode == "index" || searchMode == "exact",
      s"mode is '$mode'; a search runs in 'index' or in 'exact' mode"
    )
    val searchMetric = metric.toUpperCase(Locale.ROOT)
    require(
      outputColumns.distinct.size == outputColumns.size,
      s"Output columns repeat: ${outputColumns.mkString(", ")}"
    )
    require(
      !outputColumns.exists(Reserved),
      s"Output columns take the names the result already has: ${Reserved.toSeq.sorted.mkString(", ")}"
    )
    filter.foreach(PlanParser.parse)

    val caseInsensitive = new CaseInsensitiveStringMap(options.asJava)
    val table = MilvusTables.load(
      caseInsensitive,
      None,
      SnapshotReference.Configured
    )
    val field = table.snapshot.schema.fields
      .find(_.name == vectorColumn)
      .getOrElse(
        throw new IllegalArgumentException(
          s"The collection has no vector field '$vectorColumn'"
        )
      )
    val layout = VectorLayout.of(field.dataType, dimensionOf(field))
    checkMetric(searchMetric, layout, searchMode)
    if (searchMode == "index") {
      require(
        field.dataType == DataType.FloatVector && !field.nullable,
        s"Index search currently reads a non-nullable FloatVector; '$vectorColumn' is ${field.dataType}"
      )
    }
    SearchQueries.check(queries.schema, layout)
    val limits = SearchLimits.from(options)
    val outputSchema = outputSchemaOf(table.schema(), outputColumns)

    val partitions = plannedPartitions(table, caseInsensitive)
    val tasks = partitions.map(_.task)
    if (searchMode == "index") {
      SegmentIndexHandle.check(
        tasks,
        field.fieldID,
        searchMetric,
        allowUnindexed
      )
    }

    val selected = SearchQueries.selected(queries)
    val queryCount = selected.count()
    require(queryCount > 0, "A search needs at least one query")
    require(
      queryCount <= Int.MaxValue,
      s"A search takes at most ${Int.MaxValue} queries at a time, not $queryCount"
    )
    // `milvus.search.vectors.max.bytes` is what one executor may hold, and an
    // executor runs several tasks at once in one JVM, so the budget a task
    // plans against is the executor's divided by the slots that share it. The
    // planner and the task's own check take the same value, or a set sized to
    // fit would be rejected while it ran.
    val slots = MilvusSearch.taskSlotsPerExecutor(spark)
    val vectorsPerTask =
      math.max(layout.rowBytes.toLong, limits.vectorsMaxBytes / slots)
    val plan = SearchPlan.of(
      tasks,
      layout,
      math.max(1, spark.sparkContext.defaultParallelism),
      queryCount.toInt,
      k,
      limits.groupMaxBytes,
      vectorsPerTask
    )
    val spec = SegmentSetSearch.Spec(
      vectorColumn,
      layout,
      field.fieldID,
      field.nullable,
      k,
      searchMetric,
      searchMode,
      filter,
      searchParameters,
      allowUnindexed,
      vectorsPerTask,
      MilvusOption(caseInsensitive).readLimits.arrowMaxBytes
    )

    val metrics = SearchMetrics.create(spark.sparkContext)
    // What the first stage has to get through, so its progress has a
    // denominator. An index probe visits part of a segment and Knowhere does
    // not say how much, so index mode has no pair total and counts segment
    // searches instead.
    val comparedPairsTotal =
      if (searchMode != "exact") 0L
      else queryCount * tasks.flatMap(_.snapshotRows).sum
    val segmentSearchesTotal =
      plan.groups.size.toLong * plan.sets.map(_.size.toLong).sum
    val queryBytes = SearchQueries.bytes(queryCount, layout)
    logInfo(
      s"Search plan: mode=$searchMode, metric=$searchMetric, topK=$k, " +
        s"queries=$queryCount, queryBytes=$queryBytes delivered by " +
        s"${if (queryBytes <= limits.queriesMaxBytes) "broadcast" else "shuffle"}, " +
        s"queryGroups=${plan.groups.size}, segments=${tasks.size} in " +
        s"${plan.tasks} sets, work=${segmentSearchesTotal} segment searches" +
        (if (comparedPairsTotal > 0L)
           s" over $comparedPairsTotal distance pairs"
         else "")
    )
    // Which counter the percentage comes from, and when there is none.
    // Compared pairs rise with every batch; in index mode segment searches
    // rise with every call, because a probe searches a segment in one. An
    // exact scan whose snapshot carries no row count has neither, and says so
    // by reporting counts without a share.
    val share =
      if (comparedPairsTotal > 0L) Some(SearchMetrics.ComparedPairs)
      else if (searchMode == "index") Some(SearchMetrics.SegmentSearches)
      else None
    val progress =
      new SearchProgress(
        comparedPairsTotal,
        plan.groups.size,
        plan.sets.map(_.size).sum,
        share
      )
    progress.announce()
    spark.sparkContext.addSparkListener(progress)
    val hits =
      if (plan.isEmpty) empty(spark)
      else
        merged(
          spark.createDataFrame(
            candidates(
              spark,
              selected,
              partitions,
              plan,
              spec,
              layout,
              limits,
              metrics
            ),
            SegmentSetSearch.CandidateSchema
          ),
          k,
          searchMetric
        )
    if (outputColumns.isEmpty) hits
    else
      spark.createDataFrame(
        SearchTake.rows(
          hits,
          outputSchema,
          partitions
            .map(partition => partition.task.segmentId -> partition)
            .toMap,
          spec.arrowMaxBytes,
          metrics
        ),
        StructType(hits.schema.fields ++ outputSchema.fields)
      )
  }

  /** One query, which is the same search over a query set of one row. */
  def search(
      spark: SparkSession,
      options: Map[String, String],
      vectorColumn: String,
      queryVector: Array[Float],
      k: Int,
      metric: String,
      mode: String,
      searchParameters: Map[String, String],
      filter: Option[String],
      outputColumns: Seq[String],
      allowUnindexed: Boolean
  ): DataFrame = {
    require(
      queryVector != null && queryVector.nonEmpty,
      "A query vector must hold values"
    )
    import spark.implicits._
    val queries = Seq((0L, queryVector))
      .toDF(SearchQueries.IdColumn, SearchQueries.VectorColumn)
    search(
      spark,
      options,
      queries,
      vectorColumn,
      k,
      metric,
      mode,
      searchParameters,
      filter,
      outputColumns,
      allowUnindexed
    )
  }

  /** The candidates of the first stage, whichever way the query set travels.
    *
    * A set that fits `milvus.search.queries.max.bytes` is collected on the
    * driver and broadcast; a larger one is packed by group on the executors and
    * travels with the shuffle, never through the driver (section 2.1).
    */
  private def candidates(
      spark: SparkSession,
      selected: DataFrame,
      partitions: Seq[MilvusInputPartition],
      plan: SearchPlan.Plan,
      spec: SegmentSetSearch.Spec,
      layout: VectorLayout,
      limits: SearchLimits,
      metrics: SearchMetrics
  ): RDD[Row] = {
    val byId =
      partitions.map(partition => partition.task.segmentId -> partition).toMap
    val sets = plan.sets.map(_.map(task => byId(task.segmentId)))
    val setsRdd = spark.sparkContext.parallelize(sets, plan.tasks)
    val groups = plan.groups
    val queryBytes = SearchQueries.bytes(
      groups.map(_.queries.toLong).sum,
      layout
    )
    if (queryBytes <= limits.queriesMaxBytes) {
      val rows = selected.collect().toSeq
      val (ids, vectors) = SearchQueries.pack(rows, layout, spec.metric)
      SearchQueries.checkUnique(ids)
      val delivered = spark.sparkContext.broadcast((ids, vectors))
      setsRdd.flatMap { set =>
        val (allIds, allVectors) = delivered.value
        SegmentSetSearch.run(
          set,
          spec,
          groups.iterator.map(group =>
            SearchQueries.Group(
              allIds.slice(group.firstQuery, group.untilQuery),
              allVectors,
              group.firstQuery
            )
          ),
          groups.size,
          metrics
        )
      }
    } else {
      // One partition of groups, so a task pairs its segment set with every
      // group and reads its segments once; a group-per-partition cartesian
      // would open the same segments again for each group.
      val delivered = packedGroups(selected, plan, spec, layout)
      setsRdd.cartesian(delivered).mapPartitions { pairs =>
        if (pairs.isEmpty) Iterator.empty
        else {
          val paired = pairs.buffered
          SegmentSetSearch.run(
            paired.head._1,
            spec,
            paired.map(_._2),
            groups.size,
            metrics
          )
        }
      }
    }
  }

  /** The query set packed one group per row, on the executors, in a single
    * partition.
    *
    * `zipWithIndex` gives every query the position that decides its group, so
    * the groups are the same ones the driver planned, and a group's rows are
    * packed in query order.
    *
    * The `coalesce` is what makes the cartesian with the segment sets produce
    * one task per set instead of one per (set, group) pair, so a task receives
    * every group and reads its segments once — which is what
    * `SegmentSetSearch.run` holds the set in memory for. Its cost is that the
    * shuffle read and the packing run in one task rather than `plan.groups`
    * tasks.
    *
    * The `persist` is what keeps that one task's work from happening once per
    * segment set. Without it the cartesian recomputes this side for every left
    * partition, and since an executor runs its tasks in one JVM, the query set
    * is packed and held once per concurrent task rather than once.
    */
  private[read] def packedGroups(
      selected: DataFrame,
      plan: SearchPlan.Plan,
      spec: SegmentSetSearch.Spec,
      layout: VectorLayout
  ): RDD[SearchQueries.Group] = {
    val repeated = selected
      .groupBy(col(SearchQueries.IdColumn))
      .count()
      .filter(col("count") > 1)
      .limit(5)
      .collect()
    require(
      repeated.isEmpty,
      s"Query ids repeat in the query set: ${repeated.map(_.getLong(0)).mkString(", ")}"
    )
    val size = plan.groups.head.queries
    val metric = spec.metric
    selected.rdd
      .map(row => SearchQueries.pack(Seq(row), layout, metric))
      .zipWithIndex()
      .map { case ((ids, vectors), position) =>
        (position / size, (position, ids.head, vectors))
      }
      .groupByKey(plan.groups.size)
      .map { case (_, queries) =>
        val ordered = queries.toSeq.sortBy(_._1)
        val vectors = new Array[Byte](ordered.size * layout.rowBytes)
        ordered.zipWithIndex.foreach { case ((_, _, packed), index) =>
          System.arraycopy(
            packed,
            0,
            vectors,
            index * layout.rowBytes,
            layout.rowBytes
          )
        }
        SearchQueries.Group(ordered.map(_._2).toArray, vectors, 0)
      }
      .coalesce(1)
      // Stored, because `CartesianRDD` takes the right side's iterator once
      // per left partition and computes it again each time. The tasks of one
      // executor run in one JVM, so without this every one of them packs and
      // holds its own copy of the whole query set: an 800 MiB set at
      // `local[6]` measured 7.06 GB of query bytes against an 8 GiB heap,
      // where one copy is 1.17 GB. Stored once, the block's values are the
      // objects every task reads, which is what the collected route gets from
      // the broadcast. Memory only: a level that spills would hand each task
      // its own deserialized copy and put the six back.
      .persist(StorageLevel.MEMORY_ONLY)
  }

  /** How many tasks of this job share one executor's memory at once.
    *
    * `spark.executor.cores` says it wherever executors are separate processes.
    * A local master has no executors: the driver runs the whole job, and
    * `local[n]` means those n tasks share this one JVM, which is what
    * `defaultParallelism` reports. Anything else with the setting absent is
    * taken as one slot, which is Spark's own default and errs towards a larger
    * per-task budget rather than a smaller one.
    */
  private[read] def taskSlotsPerExecutor(spark: SparkSession): Int = {
    val configured = spark.conf
      .getOption("spark.executor.cores")
      .flatMap(value => scala.util.Try(value.trim.toInt).toOption)
      .filter(_ > 0)
    val local = spark.sparkContext.master.startsWith("local[") ||
      spark.sparkContext.master == "local"
    configured.getOrElse(
      if (local) math.max(1, spark.sparkContext.defaultParallelism) else 1
    )
  }

  /** Every query's candidates become its global top-k, best first. */
  private[read] def merged(
      candidates: DataFrame,
      k: Int,
      metric: String
  ): DataFrame = {
    val topK = udaf(new TopKAggregator(k, metric))
    candidates
      .groupBy(col("query_id"))
      .agg(
        topK(col("segment_id"), col("row_offset"), col("score")).as("top")
      )
      .select(col("query_id"), explode(col("top.hits")).as("hit"))
      .select(
        col("query_id"),
        col("hit.rank").as("rank"),
        col("hit.score").as("_score"),
        col("hit.segmentId").as("_segment_id"),
        col("hit.rowOffset").as("_row_offset")
      )
  }

  private def empty(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.emptyDataset[SearchResult].toDF()
  }

  private def plannedPartitions(
      table: com.zilliz.spark.connector.table.MilvusTable,
      options: CaseInsensitiveStringMap
  ): Seq[MilvusInputPartition] =
    table
      .newScanBuilder(options)
      .build()
      .toBatch
      .planInputPartitions()
      .toSeq
      .map(_.asInstanceOf[MilvusInputPartition])

  private def dimensionOf(field: FieldSchema): Int = field.typeParams
    .find(_.key == "dim")
    .map(_.value.toInt)
    .getOrElse(
      throw new IllegalArgumentException(
        s"Field '${field.name}' declares no dimension"
      )
    )

  private def checkMetric(
      metric: String,
      layout: VectorLayout,
      mode: String
  ): Unit = {
    val supported =
      if (layout.elementType == VectorElementType.Bit) Set("HAMMING", "JACCARD")
      else Set("L2", "IP", "COSINE")
    require(
      supported.contains(metric),
      s"A ${layout.elementType} field takes ${supported.toSeq.sorted
          .mkString(" or ")}, not $metric"
    )
    require(
      mode != "index" || Set("L2", "IP", "COSINE").contains(metric),
      s"Index search takes L2, IP or COSINE, not $metric"
    )
  }

  private def outputSchemaOf(
      table: StructType,
      outputColumns: Seq[String]
  ): StructType = StructType(
    outputColumns.map(name =>
      table.fields
        .find(_.name == name)
        .getOrElse(
          throw new IllegalArgumentException(
            s"The collection has no column '$name'"
          )
        )
    )
  )
}

/** The columns every search returns, before the output columns. */
private[read] case class SearchResult(
    query_id: Long,
    rank: Int,
    _score: Double,
    _segment_id: Long,
    _row_offset: Long
)
