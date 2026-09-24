package com.zilliz.spark.connector.read

import java.util.{Arrays, Locale}
import scala.jdk.CollectionConverters._

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.expr.PlanParser
import com.zilliz.milvus.storage.index.{
  CandidateBytes,
  MachineResources,
  SearchPlan
}
import com.zilliz.milvus.storage.read.exec.SegmentIndexHandle
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.{VectorElementType, VectorLayout}
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.options.{
  MilvusOption,
  SearchLimits,
  SearchResources,
  SnapshotReference,
  TaskResources
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

  /** What a search returns before the output columns, and the one place that
    * shape is written down: [[SearchResult]] names the columns, so an empty
    * result and a merged one cannot drift apart.
    */
  private[read] val HitSchema: StructType =
    Encoders.product[SearchResult].schema

  /** How many queries one merge partition is meant to take. The merge walks
    * sorted lists, so a partition's work is linear in the candidates it reads;
    * this only keeps a partition's reduce-side map from holding more than a few
    * thousand queries' answers at once on a cluster whose parallelism is far
    * below its query count.
    */
  private[read] val QueriesPerMergePartition: Int = 4096

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
    // A frame that is plain Parquet files is read by the tasks themselves: the
    // footers give the count, and the one job left is the id uniqueness
    // check, over the id column alone (candidates below).
    val files =
      if (limits.queriesDirect) QueryFiles.of(spark, selected) else None
    val queryCount = files.map(_.queries).getOrElse(selected.count())
    require(queryCount > 0, "A search needs at least one query")
    require(
      queryCount <= Int.MaxValue,
      s"A search takes at most ${Int.MaxValue} queries at a time, not $queryCount"
    )
    // How many first-stage tasks run at once, and what one of them may keep
    // (docs/design/architecture/vector-search.html section 2.1,
    // search-resources.html section 3.3). An index search declares that its
    // tasks take every core of their executor, so one runs per executor;
    // otherwise a task takes `spark.task.cpus`.
    val resources = TaskResources.of(spark)
    val searchProfile =
      if (searchMode == "index") resources.wholeExecutor else None
    val slots = if (searchProfile.nonEmpty) 1 else resources.tasksPerExecutor
    val concurrency = resources.executors * slots
    val (memoryLimit, heap) = MilvusSearch.executorMemory(spark)
    val segmentBudget = SearchResources.segmentBudget(
      memoryLimit,
      heap,
      slots,
      limits.segmentsMaxBytes,
      index = searchMode == "index"
    )
    val queryBudget =
      SearchResources.queryBudget(heap, memoryFraction(spark), slots)
    val blockBytes = SearchResources
      .exactScanBatch(
        if (caseInsensitive.containsKey(MilvusOption.ReadBatchMaxBytes))
          Some(MilvusOption(caseInsensitive).readLimits.batchMaxBytes)
        else None
      )
      .bytes
    // What a segment costs the task that searches it: the index the snapshot
    // recorded, as Knowhere holds it once loaded, or the vectors an exact scan
    // reads a block at a time.
    val footprint: SegmentReadTask => SearchPlan.Footprint = task =>
      (if (searchMode == "index")
         SegmentIndexHandle.select(
           task,
           field.fieldID,
           searchMetric,
           allowUnindexed
         )
       else None) match {
        case Some(index) if index.serializedSize > 0L =>
          SearchPlan.Footprint.index(index.serializedSize)
        case Some(_) => SearchPlan.Footprint.unsizedIndex
        case None    => SearchPlan.Footprint.vectors(task, layout, blockBytes)
      }
    val queryBytes = SearchQueries.bytes(queryCount, layout)
    val plan = SearchPlan.of(
      tasks,
      layout,
      concurrency,
      queryCount.toInt,
      k,
      limits.groupMaxBytes,
      SearchPlan.Budget(segmentBudget.bytes, queryBudget.bytes),
      footprint,
      shuffled = files.isEmpty && queryBytes > limits.queriesMaxBytes,
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
      plan.capacity,
      MilvusOption(caseInsensitive).readLimits.arrowMaxBytes,
      slots,
      // A block the call named is used as it is; otherwise each executor
      // chooses one for its own machine when it opens a segment.
      if (caseInsensitive.containsKey(MilvusOption.ReadBatchMaxBytes))
        Some(MilvusOption(caseInsensitive).readLimits.batchMaxBytes)
      else None,
      plan.resident
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
    // The budgets and how the plan stands against them. A native allocation
    // that fails does not surface as a Java exception, so this line is what
    // there is to read afterwards (search-resources.html sections 3.3, 3.4).
    val needs = plan.needs
      .map(need =>
        s"queries kept needs heap=${need.queriesHeap} offheap=${need.queriesOffHeap}, " +
          s"segments kept needs offheap=${need.segmentsOffHeap}"
      )
      .getOrElse("no segments")
    val over = plan.kept.count(_.exists(_ > plan.capacity))
    logInfo(
      s"Search budget: ${resources.describe}; $concurrency tasks at once " +
        s"($slots per executor${if (searchProfile.nonEmpty) ", each taking every core"
          else ""}); " +
        s"segments ${segmentBudget.reason}; queries ${queryBudget.reason}; $needs; " +
        s"kept=${plan.resident}, sets=${plan.sets.size}" +
        (if (plan.resident == SearchPlan.Resident.Segments)
           s", capacity=${plan.capacity}, " +
             (if (over == 0) "no set known to stream"
              else s"$over sets stream once per query group")
         else "") +
        s", ${plan.estimated.size} segments estimated at half the budget"
    )
    // The stage that packs the query groups holds one group's ids and vectors
    // per task on the heap, outside what Spark manages, so no more of them run
    // at once on an executor than that heap holds with room to spare.
    val packProfile =
      if (
        files.nonEmpty || queryBytes <= limits.queriesMaxBytes ||
        plan.groups.isEmpty
      ) None
      else {
        val perGroup = plan.groups.map(_.queries.toLong).max *
          (SearchPlan.QueryIdBytes + layout.rowBytes.toLong)
        val heapRoom = SearchResources
          .queryBudget(heap, memoryFraction(spark), 1)
          .bytes
        val fits = math.max(1L, heapRoom / (perGroup + perGroup / 2))
        resources.atMost(math.min(fits, Int.MaxValue.toLong).toInt)
      }
    logInfo(
      s"Search plan: mode=$searchMode, metric=$searchMetric, topK=$k, " +
        s"queries=$queryCount, queryBytes=$queryBytes delivered by " +
        s"${files match {
            case Some(direct) =>
              s"tasks reading ${direct.files.size} parquet files"
            case None if queryBytes <= limits.queriesMaxBytes => "broadcast"
            case None                                         => "shuffle"
          }}, " +
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
          spark,
          candidates(
            spark,
            selected,
            files,
            partitions,
            plan,
            spec,
            layout,
            limits,
            metrics,
            searchProfile,
            packProfile
          ),
          k,
          searchMetric,
          mergePartitions(spark, queryCount, plan.tasks)
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
    * A set that fits `milvus.search.queries.max.bytes` is packed on the
    * executors, concatenated on the driver and broadcast, and every executor
    * keeps it whole. A larger one is packed by group on the executors, never
    * through the driver, and each task reads the groups of its range one at a
    * time (section 2.1).
    */
  private def candidates(
      spark: SparkSession,
      selected: DataFrame,
      files: Option[QueryFiles],
      partitions: Seq[MilvusInputPartition],
      plan: SearchPlan.Plan,
      spec: SegmentSetSearch.Spec,
      layout: VectorLayout,
      limits: SearchLimits,
      metrics: SearchMetrics,
      searchProfile: Option[ResourceProfile],
      packProfile: Option[ResourceProfile]
  ): RDD[(Long, Array[Byte])] = {
    val kept = plan.kept.toVector
    val byId =
      partitions.map(partition => partition.task.segmentId -> partition).toMap
    val sets = plan.sets.map(_.map(task => byId(task.segmentId)))
    val groups = plan.groups
    val ranges = plan.queryRanges
    val queryBytes = SearchQueries.bytes(
      groups.map(_.queries.toLong).sum,
      layout
    )
    if (files.nonEmpty) {
      // The files are read by the tasks and the driver never holds the set.
      // The id uniqueness check is the one pass over the frame that remains,
      // and it reads the id column alone: 2 MB for 250,000 queries.
      SearchQueries.checkUnique(
        selected
          .select(selected.col(SearchQueries.IdColumn))
          .queryExecution
          .toRdd
          .map(_.getLong(0))
          .collect()
      )
      val direct = files.get
      val work = for {
        (set, index) <- sets.zipWithIndex
        range <- ranges
      } yield (set, kept.lift(index).flatten, range)
      val keeps = plan.resident == SearchPlan.Resident.Queries
      val searched = spark.sparkContext.parallelize(work, plan.tasks).flatMap {
        case (set, planned, range) =>
          // A task that keeps its queries decodes them by row group on
          // several threads, straight into the group matrices; one that
          // streams its groups reads them in order as it goes.
          if (keeps || range.size == 1)
            SegmentSetSearch.runDecoded(
              set,
              spec,
              allocator =>
                direct.decode(
                  range,
                  groups,
                  layout,
                  spec.metric,
                  allocator,
                  QueryFiles.decodeThreads(direct.rowGroups.size)
                ),
              range.size,
              metrics
            )
          else
            SegmentSetSearch.run(
              set,
              spec,
              direct.groups(range, groups, layout, spec.metric),
              range.size,
              planned,
              metrics
            )
      }
      searchProfile.fold(searched)(searched.withResources)
    } else if (queryBytes <= limits.queriesMaxBytes) {
      // Packed where the rows are read: the driver gets bytes, not boxed
      // vectors, and eight executors pack in parallel instead of one thread.
      val (ids, vectors) =
        SearchQueries.packOnExecutors(selected, layout, spec.metric)
      require(
        ids.length.toLong == groups.map(_.queries.toLong).sum,
        s"The query set packed to ${ids.length} queries, but the plan counted ${groups.map(_.queries.toLong).sum}"
      )
      SearchQueries.checkUnique(ids)
      val delivered = spark.sparkContext.broadcast((ids, vectors))
      val work = for {
        (set, index) <- sets.zipWithIndex
        range <- ranges
      } yield (set, kept.lift(index).flatten, range)
      val searched = spark.sparkContext.parallelize(work, plan.tasks).flatMap {
        case (set, planned, range) =>
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
            planned,
            metrics
          )
      }
      searchProfile.fold(searched)(searched.withResources)
    } else {
      val delivered = packedGroups(selected, plan, spec, layout, packProfile)
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
      val searched = new SearchQueryRanges(delivered, sets.size, ranges)
        .mapPartitionsWithIndex { (index, streamed) =>
          SegmentSetSearch.run(
            held.value(index / rangeCount),
            spec,
            streamed,
            rangeSizes(index % rangeCount),
            kept.lift(index / rangeCount).flatten,
            metrics
          )
        }
      searchProfile.fold(searched)(searched.withResources)
    }
  }

  /** The query set packed on the executors, one partition per query group, each
    * the output of a shuffle.
    *
    * `zipWithIndex` gives every query the position that decides its group and
    * its place in the group, so the groups are the same ones the driver planned
    * and a group's rows are packed in query order without sorting. One task
    * packs one group, and the groups are packed in parallel.
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
      layout: VectorLayout,
      profile: Option[ResourceProfile] = None
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
    // Where each planned group starts. The byte limit cuts equal groups and a
    // shorter last one, an even cut puts the longer groups first (decision
    // 32), so a position finds its group among the starts, not by dividing.
    val starts = plan.groups.map(_.firstQuery.toLong).toArray
    val metric = spec.metric
    val rowBytes = layout.rowBytes
    val sizes = plan.groups.map(_.queries).toVector
    // A row arrives, is written where it belongs and is done with: its
    // position says both its group and its place in the group, so the task
    // holds the group's arrays and the row in hand. Holding the group's rows
    // before copying them holds the group twice. `groupByKey` did that (a heap
    // dump of a failed 1 GiB query set found 2.03 GB in `byte[4096]` beside
    // the arrays they were being copied into), and so did sorting each group
    // by position first: four packing tasks of 232 MB groups ran a 4 GiB
    // executor heap out (UAT chenbiao-qs2c-31m-8g-ef151-0).
    val byGroup = new Partitioner {
      override def numPartitions: Int = sizes.size
      override def getPartition(key: Any): Int = {
        val found = Arrays.binarySearch(starts, key.asInstanceOf[Long])
        if (found >= 0) found else -found - 2
      }
    }
    // The packed group keeps its group's partition through the second shuffle.
    val byIndex = new Partitioner {
      override def numPartitions: Int = sizes.size
      override def getPartition(key: Any): Int = key.asInstanceOf[Int]
    }
    val packed = selected.rdd
      .map(row => SearchQueries.pack(Seq(row), layout, metric))
      .zipWithIndex()
      .map { case ((ids, vectors), position) =>
        (position, (ids.head, vectors))
      }
      .partitionBy(byGroup)
      .mapPartitionsWithIndex { (index, entries) =>
        val queries = sizes(index)
        val first = starts(index)
        val ids = new Array[Long](queries)
        val vectors = new Array[Byte](queries * rowBytes)
        var placed = 0
        entries.foreach { case (position, (id, packed)) =>
          val at = position - first
          require(
            at >= 0L && at < queries,
            s"Query at position $position does not belong to group $index"
          )
          ids(at.toInt) = id
          System.arraycopy(packed, 0, vectors, at.toInt * rowBytes, rowBytes)
          placed += 1
        }
        require(
          placed == queries,
          s"Query group $index takes $queries queries and was given $placed"
        )
        Iterator((index, SearchQueries.Group(ids, vectors, 0)))
      }
    profile
      .fold(packed)(packed.withResources)
      .partitionBy(byIndex)
      .values
  }

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

  /** `spark.memory.fraction`: the share of the heap Spark manages, the rest
    * being where a task's own objects live.
    */
  private def memoryFraction(spark: SparkSession): Double = spark.conf
    .getOption("spark.memory.fraction")
    .flatMap(value => scala.util.Try(value.trim.toDouble).toOption)
    .getOrElse(0.6)

  /** How many partitions the merge runs in: one for each task slot the job has,
    * and never more than there are queries. The merge is an RDD shuffle rather
    * than a SQL aggregation, so nothing downstream re-partitions it and
    * `spark.sql.shuffle.partitions` does not apply.
    */
  private[read] def mergePartitions(
      spark: SparkSession,
      queries: Long,
      tasks: Int
  ): Int = {
    require(queries > 0, s"A search needs at least one query: $queries")
    require(tasks > 0, s"A search needs at least one task: $tasks")
    // At least one partition for every task slot the job has and for every
    // first-stage task, so no executor sits out the merge; more when the
    // queries would otherwise pile up in one partition; never more than there
    // are queries, since a query is one key and cannot be split.
    val slots = math.max(1, spark.sparkContext.defaultParallelism)
    val byQueries =
      (queries + QueriesPerMergePartition - 1L) / QueriesPerMergePartition
    val wanted = math.max(math.max(slots.toLong, tasks.toLong), byQueries)
    math.max(1L, math.min(queries, wanted)).toInt
  }

  /** Every query's candidates become its global top-k, best first.
    *
    * Each task already merged its own segments, so what arrives here is one
    * packed answer per (query, task) and the merge is a walk over two sorted
    * lists, not an aggregation over candidates. Map-side combining is off on
    * purpose: a task emits each of its queries once, so combining on the map
    * side would hold every query's answer until the task ends instead of
    * writing each one as it is packed (section 2.6).
    */
  private[read] def merged(
      spark: SparkSession,
      candidates: RDD[(Long, Array[Byte])],
      k: Int,
      metric: String,
      partitions: Int
  ): DataFrame = {
    require(partitions > 0, s"The merge needs a partition: $partitions")
    val hits = candidates
      .combineByKeyWithClassTag[Array[Byte]](
        (packed: Array[Byte]) => packed,
        (merged: Array[Byte], packed: Array[Byte]) =>
          CandidateBytes.merge(merged, packed, k, metric),
        (left: Array[Byte], right: Array[Byte]) =>
          CandidateBytes.merge(left, right, k, metric),
        new HashPartitioner(partitions),
        mapSideCombine = false
      )
      .flatMap { case (query, packed) => rows(query, packed) }
    spark.createDataFrame(hits, HitSchema)
  }

  /** One query's answer as the rows the result contract promises: rank from
    * one, best first.
    */
  private[read] def rows(query: Long, packed: Array[Byte]): Seq[Row] = {
    val hits = Vector.newBuilder[Row]
    var rank = 1
    CandidateBytes.foreach(packed) { (segmentId, rowOffset, score) =>
      hits += Row(query, rank, score, segmentId, rowOffset)
      rank += 1
    }
    hits.result()
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
