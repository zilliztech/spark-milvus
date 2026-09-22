package com.zilliz.spark.connector.read

import java.util.Locale
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, explode, udaf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.Partitioner

import com.zilliz.milvus.storage.expr.PlanParser
import com.zilliz.milvus.storage.index.{MachineResources, SearchPlan}
import com.zilliz.milvus.storage.read.exec.SegmentIndexHandle
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.options.{
  MilvusOption,
  SearchLimits,
  SearchResources,
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

    SearchQueries.check(queries.schema, layout)
    val limits = SearchLimits.from(options)
    val outputSchema = outputSchemaOf(table.schema(), outputColumns)

    val partitions = SnapshotPartitions.of(table, caseInsensitive)
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
    // The vectors a task keeps resident come out of the executor's off-heap
    // room, which its tasks share, so the budget a task plans against is the
    // executor's divided by the slots. `milvus.search.vectors.max.bytes` names
    // the executor's total when the call sets it; otherwise it is derived from
    // the executor's memory limit and heap. The planner and the task take the
    // same value; a set that turns out larger streams instead of holding.
    val slots = MilvusSearch.taskSlotsPerExecutor(spark)
    val (memoryLimit, heap) = MilvusSearch.executorMemory(spark)
    val budget = SearchResources.vectorsBudget(
      memoryLimit,
      heap,
      slots,
      limits.vectorsMaxBytes
    )
    val vectorsPerTask = math.max(layout.rowBytes.toLong, budget.bytes)
    val plan = SearchPlan.of(
      tasks,
      layout,
      math.max(1, spark.sparkContext.defaultParallelism),
      queryCount.toInt,
      k,
      limits.groupMaxBytes,
      vectorsPerTask,
      // The batched distance entry is single-threaded on its task, so an
      // exact search is as parallel as its tasks (decision 28).
      splitQueries = searchMode == "exact"
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
      MilvusOption(caseInsensitive).readLimits.arrowMaxBytes,
      slots,
      // A block the call named is used as it is; otherwise each executor
      // chooses one for its own machine when it opens a segment.
      if (caseInsensitive.containsKey(MilvusOption.ReadBatchMaxBytes))
        Some(MilvusOption(caseInsensitive).readLimits.batchMaxBytes)
      else None
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
    // The budget and how the sets stand against it: a set known to be over
    // it streams once per query group; a set with an estimated segment is
    // decided when the task reads it. One group holds nothing either way.
    val known = plan.sets.flatMap(SearchPlan.knownBytes(_, layout))
    val over = known.count(_ > vectorsPerTask)
    logInfo(
      s"Search budget: vectorsPerTask=$vectorsPerTask (${budget.reason}), " +
        s"sets=${plan.sets.size}, setBytes=" +
        (if (known.isEmpty) "unknown"
         else s"${known.min}..${known.max}") +
        s" known for ${known.size} sets, ${plan.estimated.size} segments " +
        s"estimated at half the budget, " +
        (if (plan.groups.size == 1) "one query group so nothing is held"
         else if (over == 0) "no set known to stream"
         else s"$over sets stream once per query group")
    )
    // What an index search asks of the executor's off-heap memory, from the
    // sizes the snapshot recorded: an index is copied into a BinarySet and then
    // deserialized, so loading peaks at twice its bytes, and the slots load at
    // once. Nothing is refused on this; a native allocation that fails does
    // not surface as a Java exception, so this line is what there is to read
    // afterwards (docs/design/architecture/search-resources.html section 3.4).
    if (searchMode == "index") {
      val perSet = plan.sets.map(set =>
        set
          .flatMap(task =>
            SegmentIndexHandle
              .select(task, field.fieldID, searchMetric, allowUnindexed)
          )
          .map(_.serializedSize)
          .filter(_ > 0L)
          .sum
      )
      val perTask = if (perSet.isEmpty) 0L else perSet.max
      val perExecutor = perTask * 2L * slots
      val offHeap = memoryLimit.map(limit =>
        math.max(0L, limit - heap - SearchResources.JvmReserveBytes)
      )
      val line =
        if (perTask == 0L)
          "Search index memory: the snapshot records no index sizes, nothing to forecast"
        else
          s"Search index memory: indexBytesPerTask=$perTask (largest set), " +
            s"loadPeak=${perTask * 2L} (BinarySet and the deserialized index), " +
            s"perExecutor=$perExecutor over $slots slots, " +
            s"offHeapAvailable=${offHeap.map(_.toString).getOrElse("unknown")}"
      if (offHeap.exists(_ < perExecutor))
        logWarning(
          s"$line; the executor's off-heap room is smaller than the peak, and a native " +
            "allocation that fails does not raise a Java exception"
        )
      else logInfo(line)
    }
    logInfo(
      s"Search plan: mode=$searchMode, metric=$searchMetric, topK=$k, " +
        s"queries=$queryCount, queryBytes=$queryBytes delivered by " +
        s"${if (queryBytes <= limits.queriesMaxBytes) "broadcast" else "shuffle"}, " +
        s"queryGroups=${plan.groups.size}, segments=${tasks.size} in " +
        s"${plan.sets.size} sets x ${plan.queryRanges.size} query ranges = " +
        s"${plan.tasks} tasks, work=${segmentSearchesTotal} segment searches" +
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
    * driver and broadcast, and every executor keeps it whole. A larger one is
    * packed by group on the executors, never through the driver, and each task
    * reads the groups of its range one at a time (section 2.1).
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
    val groups = plan.groups
    val ranges = plan.queryRanges
    val queryBytes = SearchQueries.bytes(
      groups.map(_.queries.toLong).sum,
      layout
    )
    if (queryBytes <= limits.queriesMaxBytes) {
      val rows = selected.collect().toSeq
      val (ids, vectors) = SearchQueries.pack(rows, layout, spec.metric)
      SearchQueries.checkUnique(ids)
      val delivered = spark.sparkContext.broadcast((ids, vectors))
      val work = for {
        set <- sets
        range <- ranges
      } yield (set, range)
      spark.sparkContext.parallelize(work, plan.tasks).flatMap {
        case (set, range) =>
          val (allIds, allVectors) = delivered.value
          SegmentSetSearch.run(
            set,
            spec,
            range.iterator.map { index =>
              val group = groups(index)
              SearchQueries.Group(
                allIds.slice(group.firstQuery, group.untilQuery),
                allVectors,
                group.firstQuery
              )
            },
            range.size,
            metrics
          )
      }
    } else {
      val delivered = packedGroups(selected, plan, spec, layout)
      require(
        delivered.getNumPartitions == groups.size,
        s"The packed query set is ${groups.size} groups, not " +
          s"${delivered.getNumPartitions} partitions"
      )
      // Task `index` is set `index / ranges` and range `index % ranges`, which
      // is how `SearchQueryRanges` numbers its partitions. The task learns its
      // set from that index before it reads a group, and the sets are
      // broadcast once for the executor rather than once for each task.
      val held = spark.sparkContext.broadcast(sets.toVector)
      val rangeCount = ranges.size
      val rangeSizes = ranges.map(_.size).toVector
      new SearchQueryRanges(delivered, sets.size, ranges)
        .mapPartitionsWithIndex { (index, streamed) =>
          SegmentSetSearch.run(
            held.value(index / rangeCount),
            spec,
            streamed,
            rangeSizes(index % rangeCount),
            metrics
          )
        }
    }
  }

  /** The query set packed on the executors, one partition per query group, each
    * the output of a shuffle.
    *
    * `zipWithIndex` gives every query the position that decides its group, so
    * the groups are the same ones the driver planned, and a group's rows are
    * packed in query order. One task packs one group, and the groups are packed
    * in parallel.
    *
    * The packed group then goes through a second shuffle, keyed by its group,
    * so that it is written once to the local disk of the executor that packed
    * it and every first-stage task that needs it reads it from there. Nothing
    * is stored in memory: [[SearchQueryRanges]] reads a task's groups one at a
    * time, and reading a group again is a shuffle read, not a second packing of
    * the query set.
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
    val rowBytes = layout.rowBytes
    val sizes = plan.groups.map(_.queries).toVector
    // A group's row arrives, is written where it belongs and is done with.
    // `groupByKey` would hold the whole group as one query's bytes at a time
    // and then build the group's bytes beside it, which is the group twice
    // over: a heap dump of a failed 1 GiB query set found 2.03 GB in
    // `byte[4096]` beside the arrays they were being copied into. Partitioning
    // by group and sorting by position inside it puts the rows in the order
    // they are wanted, so the peak is the group plus the row in hand.
    val byGroup = new Partitioner {
      override def numPartitions: Int = sizes.size
      override def getPartition(key: Any): Int =
        key.asInstanceOf[(Long, Long)]._1.toInt
    }
    // The packed group keeps its group's partition through the second shuffle.
    val byIndex = new Partitioner {
      override def numPartitions: Int = sizes.size
      override def getPartition(key: Any): Int = key.asInstanceOf[Int]
    }
    selected.rdd
      .map(row => SearchQueries.pack(Seq(row), layout, metric))
      .zipWithIndex()
      .map { case ((ids, vectors), position) =>
        ((position / size, position), (ids.head, vectors))
      }
      .repartitionAndSortWithinPartitions(byGroup)
      .mapPartitionsWithIndex { (index, entries) =>
        val queries = sizes(index)
        val ids = new Array[Long](queries)
        val vectors = new Array[Byte](queries * rowBytes)
        var at = 0
        entries.foreach { case (_, (id, packed)) =>
          require(
            at < queries,
            s"Query group $index takes $queries queries and was given more"
          )
          ids(at) = id
          System.arraycopy(packed, 0, vectors, at * rowBytes, rowBytes)
          at += 1
        }
        require(
          at == queries,
          s"Query group $index takes $queries queries and was given $at"
        )
        Iterator((index, SearchQueries.Group(ids, vectors, 0)))
      }
      .partitionBy(byIndex)
      .values
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
  /** The executor's memory: its limit, when it can be known, and its heap.
    *
    * In local mode the executor is this JVM, so the limit is the cgroup's or
    * the machine's and the heap is this runtime's. On a cluster the driver
    * cannot read the executor's cgroup, but the container Spark asked for is
    * its limit: `spark.executor.memory` plus the overhead and, when enabled,
    * the off-heap size (docs/design/architecture/search-resources.html section
    * 3.3).
    */
  private[read] def executorMemory(
      spark: SparkSession
  ): (Option[Long], Long) = {
    val master = spark.sparkContext.master
    if (master.startsWith("local[") || master == "local") {
      (
        MachineResources.probe().memoryLimitBytes,
        Runtime.getRuntime.maxMemory()
      )
    } else {
      def bytes(key: String, default: String): Long =
        JavaUtils.byteStringAsBytes(spark.conf.get(key, default))
      val heap = bytes("spark.executor.memory", "1g")
      val overhead =
        spark.conf.getOption("spark.executor.memoryOverhead") match {
          case Some(set) => JavaUtils.byteStringAsBytes(set)
          case None =>
            val factor = spark.conf
              .getOption("spark.executor.memoryOverheadFactor")
              .flatMap(value => scala.util.Try(value.trim.toDouble).toOption)
              .getOrElse(0.10)
            math.max(384L << 20, (heap * factor).toLong)
        }
      val offHeap =
        if (spark.conf.get("spark.memory.offHeap.enabled", "false") == "true")
          bytes("spark.memory.offHeap.size", "0")
        else 0L
      (Some(heap + overhead + offHeap), heap)
    }
  }

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
