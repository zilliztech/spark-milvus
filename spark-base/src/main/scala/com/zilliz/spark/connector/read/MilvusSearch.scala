package com.zilliz.spark.connector.read

import java.util.Arrays
import scala.jdk.CollectionConverters._

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.expr.{Expr, PredicateExpr}
import com.zilliz.milvus.storage.index.{
  CandidateBytes,
  MachineResources,
  RankingFunction,
  SearchPlan
}
import com.zilliz.milvus.storage.read.exec.SegmentIndexHandle
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.{
  MetricType,
  VectorElementType,
  VectorLayout
}
import com.zilliz.spark.connector.metrics.SearchMetrics
import com.zilliz.spark.connector.options.{
  MilvusOption,
  SearchLimits,
  SearchResources,
  TaskResources
}
import io.milvus.grpc.schema.FieldSchema

/** The two-stage search over the segments of a Milvus table input: a query set
  * in, every query's global top-k out, with the base columns the take stage
  * reads (docs/design/architecture/vector-search.html section 2.1).
  *
  * A nearest-by join over a Milvus table runs it (`NearestBySearch`); the join
  * is how a search is written (docs/design/architecture/dataframe-api.html
  * sections 2 and 4). The `exact` mode computes every distance in every
  * segment; the `index` mode searches the persisted index the snapshot pinned.
  * A hit carries `query_id`, `rank`, `_score`, `_segment_id` and `_row_offset`,
  * then the base columns asked for. A DataFrame input's search (`FrameSearch`)
  * shares the packed delivery of the query groups, the merge partitioning and
  * the reading of the executors' memory kept here.
  */
object MilvusSearch extends Logging {

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

  /** What [[execute]] gives back: the hits of every query with the output
    * columns asked for, as rows of `schema`. An iterator of these rows may hand
    * the same object back with new values, so a consumer that keeps a row
    * copies it.
    */
  private[connector] final case class Hits(
      rows: RDD[InternalRow],
      schema: StructType
  )

  /** The hit columns when some query is required to appear without a hit: the
    * columns of the hit are NULL in its row.
    */
  private[read] val NullableHitSchema: StructType = StructType(
    HitSchema.fields.map(field =>
      if (field.name == SearchQueries.IdColumn) field
      else field.copy(nullable = true)
    )
  )

  /** The two Spark stages over segments already planned: candidates from every
    * segment set, merged into each query's best k, then the output columns
    * taken for the hits alone (docs/design/architecture/vector-search.html
    * section 2.1).
    *
    * `queries` is the query set: a frame of `query_id` and `vector`, or a set
    * the driver already packed. The ids are the positions the nearest-by join
    * numbered its query rows by, so they cannot repeat and are not checked.
    * `required` names the ids that must each have a row, one with NULL hit
    * columns when nothing was found for it, which is what a LEFT OUTER
    * nearest-by join whose query rows the driver holds needs. `filter` and
    * `predicate` exclude rows before the top k: a Milvus expression, and what
    * Spark pushed into the scan whose partitions these are
    * (docs/design/architecture/dataframe-api.html section 4). An index search
    * scans exactly a segment it has no usable index for, and `metrics` is where
    * the tasks count. `function` is what the join ranks by where Knowhere does
    * not score a pair as it does (section 2).
    */
  private[connector] def execute(
      spark: SparkSession,
      options: Map[String, String],
      queries: SearchQueries.Input,
      partitions: Seq[MilvusInputPartition],
      field: FieldSchema,
      layout: VectorLayout,
      k: Int,
      searchMetric: MetricType,
      searchMode: SearchMode,
      searchParameters: Map[String, String],
      filter: Option[Expr],
      predicate: Option[PredicateExpr],
      outputSchema: StructType,
      metrics: SearchMetrics,
      function: RankingFunction,
      required: Option[Array[Long]]
  ): Hits = {
    val caseInsensitive = new CaseInsensitiveStringMap(options.asJava)
    val limits = SearchLimits.from(options)
    val tasks = partitions.map(_.task)
    if (searchMode == SearchMode.Index) {
      SegmentIndexHandle.check(tasks, field.fieldID, searchMetric)
    }

    // A set the driver packed is counted already.
    val packedOnDriver = queries.isInstanceOf[SearchQueries.Packed]
    val queryCount = queries match {
      case SearchQueries.Frame(selected) => selected.count()
      case SearchQueries.Packed(ids, _)  => ids.length.toLong
    }
    val hitSchema = if (required.isEmpty) HitSchema else NullableHitSchema
    val resultSchema = StructType(hitSchema.fields ++ outputSchema.fields)
    // The set is what a nearest-by join kept of its query rows: rows whose
    // ranking value is NULL search nothing, and all of them may be.
    if (queryCount == 0)
      return Hits(missing(spark, required, resultSchema), resultSchema)
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
      if (searchMode == SearchMode.Index) resources.wholeExecutor else None
    val slots = if (searchProfile.nonEmpty) 1 else resources.tasksPerExecutor
    val concurrency = resources.executors * slots
    val (memoryLimit, heap) = MilvusSearch.executorMemory(spark)
    val segmentBudget = SearchResources.segmentBudget(
      memoryLimit,
      heap,
      slots,
      limits.segmentsMaxBytes,
      index = searchMode == SearchMode.Index
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
      (if (searchMode == SearchMode.Index)
         SegmentIndexHandle.select(task, field.fieldID, searchMetric)
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
      shuffled = !packedOnDriver && queryBytes > limits.queriesMaxBytes,
      // The batched distance entry is single-threaded on its task, so an
      // exact search is as parallel as its tasks (decision 28).
      splitQueries = searchMode == SearchMode.Exact,
      rangesWanted = limits.queryRanges
    )
    // Between two Knowhere calls a search task checks, collects and packs its
    // candidates on as many threads as it holds cores, unless the call said.
    val collectThreads =
      if (limits.collectThreads > 0) limits.collectThreads
      else if (searchProfile.nonEmpty) resources.executorCores
      else math.max(1, resources.taskCpus)
    val spec = SegmentSetSearch.Spec(
      field.name,
      layout,
      field.fieldID,
      field.nullable,
      k,
      searchMetric,
      searchMode,
      filter,
      searchParameters,
      plan.capacity,
      MilvusOption(caseInsensitive).readLimits.arrowMaxBytes,
      slots,
      // A block the call named is used as it is; otherwise each executor
      // chooses one for its own machine when it opens a segment.
      if (caseInsensitive.containsKey(MilvusOption.ReadBatchMaxBytes))
        Some(MilvusOption(caseInsensitive).readLimits.batchMaxBytes)
      else None,
      plan.resident,
      collectThreads,
      predicate,
      Some(function)
    )

    // What the first stage has to get through, so its progress has a
    // denominator. An index probe visits part of a segment and Knowhere does
    // not say how much, so index mode has no pair total and counts segment
    // searches instead.
    val comparedPairsTotal =
      if (searchMode != SearchMode.Exact) 0L
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
        packedOnDriver || queryBytes <= limits.queriesMaxBytes ||
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
        (if (packedOnDriver) "broadcast, packed on the driver"
         else if (queryBytes <= limits.queriesMaxBytes) "broadcast"
         else "shuffle") + ", " +
        s"queryGroups=${plan.groups.size}, segments=${tasks.size} in " +
        s"${plan.sets.size} sets x ${plan.queryRanges.size} query ranges = " +
        s"${plan.tasks} tasks, collectThreads=$collectThreads, " +
        s"work=${segmentSearchesTotal} segment searches" +
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
      else if (searchMode == SearchMode.Index)
        Some(SearchMetrics.SegmentSearches)
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
    // The merge and take stages run under Spark's own task slots, so a task's
    // share of the unmanaged heap is the queries' budget at that concurrency.
    val stageHeap = SearchResources
      .queryBudget(heap, memoryFraction(spark), resources.tasksPerExecutor)
    if (plan.isEmpty && required.nonEmpty)
      return Hits(missing(spark, required, resultSchema), resultSchema)
    val hits =
      if (plan.isEmpty) empty(spark)
      else {
        val mergeParts =
          mergePartitions(spark, queryCount, plan.tasks, k, stageHeap.bytes)
        logInfo(
          s"Merge stage: $queryCount queries x ${plan.tasks} answers of $k over $mergeParts partitions, " +
            s"about ${queryCount / mergeParts} queries a partition at ${plan.tasks.toLong * k * CandidateBytes.Width} bytes each " +
            s"against ${stageHeap.bytes} bytes of heap a task"
        )
        merged(
          spark,
          candidates(
            spark,
            queries,
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
          mergeParts,
          required
        )
      }
    if (outputSchema.isEmpty) Hits(hits.queryExecution.toRdd, hits.schema)
    else {
      // What a take task buffers is every hit of its partition (queries x k
      // over the tasks), against the same heap share as the merge.
      val takePartitioning = SearchTake.partitioning(
        queryCount * k,
        math.max(1, partitions.size),
        stageHeap.bytes,
        spark.sessionState.conf.numShufflePartitions
      )
      logInfo(
        s"Take stage: ${queryCount * k} hits over ${takePartitioning.partitions} tasks" +
          s" (${takePartitioning.buckets} bucket(s) per segment), " +
          s"about ${queryCount * k / takePartitioning.partitions} hits a task at " +
          s"${SearchTake.HitRowBytes} bytes each against ${stageHeap.bytes} bytes of heap a task"
      )
      Hits(
        SearchTake.rows(
          hits,
          outputSchema,
          partitions
            .map(partition => partition.task.segmentId -> partition)
            .toMap,
          spec.arrowMaxBytes,
          metrics,
          takePartitioning
        ),
        StructType(hits.schema.fields ++ outputSchema.fields)
      )
    }
  }

  /** The candidates of the first stage, whichever way the query set travels.
    *
    * A set the driver packed is broadcast as it is. A frame that fits
    * `milvus.search.queries.max.bytes` is packed on the executors, concatenated
    * on the driver and broadcast, and every executor keeps it whole. A larger
    * one is packed by group on the executors, never through the driver, and
    * each task reads the groups of its range one at a time (section 2.1).
    */
  private def candidates(
      spark: SparkSession,
      queries: SearchQueries.Input,
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
    if (
      queries.isInstanceOf[SearchQueries.Packed] ||
      queryBytes <= limits.queriesMaxBytes
    ) {
      // Packed where the rows are read: the driver gets bytes, not boxed
      // vectors, and eight executors pack in parallel instead of one thread.
      // A set the driver packed is broadcast as it is.
      val (ids, vectors) = queries match {
        case SearchQueries.Packed(packedIds, packedVectors) =>
          (packedIds, packedVectors)
        case SearchQueries.Frame(frame) =>
          SearchQueries.packOnExecutors(frame, layout, spec.metric)
      }
      require(
        ids.length.toLong == groups.map(_.queries.toLong).sum,
        s"The query set packed to ${ids.length} queries, but the plan counted ${groups.map(_.queries.toLong).sum}"
      )
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
      // A set the driver packed went to the broadcast above.
      val selected = queries match {
        case SearchQueries.Frame(frame) => frame
        case SearchQueries.Packed(_, _) =>
          throw new IllegalStateException(
            "A query set packed on the driver is broadcast, not read again"
          )
      }
      val delivered = packedGroups(
        selected,
        plan.groups,
        spec.metric,
        layout,
        packProfile
      )
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
      groups: Seq[SearchPlan.QueryGroup],
      metric: MetricType,
      layout: VectorLayout,
      profile: Option[ResourceProfile] = None
  ): RDD[SearchQueries.Group] = {
    // Where each planned group starts. The byte limit cuts equal groups and a
    // shorter last one, an even cut puts the longer groups first (decision
    // 32), so a position finds its group among the starts, not by dividing.
    val starts = groups.map(_.firstQuery.toLong).toArray
    val rowBytes = layout.rowBytes
    val sizes = groups.map(_.queries).toVector
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
  private[read] def memoryFraction(spark: SparkSession): Double = spark.conf
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
      tasks: Int,
      k: Int,
      heapBytesPerTask: Long,
      candidateBytes: Long = CandidateBytes.Width.toLong
  ): Int = {
    require(queries > 0, s"A search needs at least one query: $queries")
    require(tasks > 0, s"A search needs at least one task: $tasks")
    require(k > 0, s"A search needs a positive k: $k")
    // At least one partition for every task slot the job has and for every
    // first-stage task, so no executor sits out the merge; more when the
    // queries would otherwise pile up in one partition; never more than there
    // are queries, since a query is one key and cannot be split.
    //
    // How many queries a partition may hold is the smaller of a fixed few
    // thousand and what the task's heap fits: every query arrives as one
    // packed answer per first-stage task, `tasks x k x Width` bytes, and the
    // reduce-side map holds them until they are merged. 100k queries at
    // k=16,384 over 15 tasks were 5.9 MB a query; 64 partitions of 1,563
    // queries each ran the 6 GiB heap out (2026-09-26 decision). Half the
    // heap share is left for the merged answers and the rows they become.
    val slots = math.max(1, spark.sparkContext.defaultParallelism)
    val bytesPerQuery = tasks.toLong * k.toLong * candidateBytes
    val byHeap =
      math.max(1L, math.max(0L, heapBytesPerTask) / 2L / bytesPerQuery)
    val perPartition = math.min(QueriesPerMergePartition.toLong, byHeap)
    val byQueries = (queries + perPartition - 1L) / perPartition
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
      metric: MetricType,
      partitions: Int,
      required: Option[Array[Long]] = None
  ): DataFrame = {
    require(partitions > 0, s"The merge needs a partition: $partitions")
    val partitioner = new HashPartitioner(partitions)
    val combined = candidates
      .combineByKeyWithClassTag[Array[Byte]](
        (packed: Array[Byte]) => packed,
        (merged: Array[Byte], packed: Array[Byte]) =>
          CandidateBytes.merge(merged, packed, k, metric),
        (left: Array[Byte], right: Array[Byte]) =>
          CandidateBytes.merge(left, right, k, metric),
        partitioner,
        mapSideCombine = false
      )
    required match {
      case None =>
        spark.createDataFrame(
          combined.flatMap { case (query, packed) => rows(query, packed) },
          HitSchema
        )
      case Some(ids) =>
        // A required id that no task found anything for has no key here; the
        // partition its key would hash to says so after its own answers.
        val wanted = spark.sparkContext.broadcast(ids)
        val hits = combined.mapPartitionsWithIndex { (index, answers) =>
          val found = new java.util.HashSet[java.lang.Long]()
          answers.flatMap { case (query, packed) =>
            found.add(query)
            rows(query, packed)
          } ++ wanted.value.iterator
            .filter(id =>
              partitioner.getPartition(id) == index && !found.contains(id)
            )
            .map(id => Row(id, null, null, null, null))
        }
        spark.createDataFrame(hits, NullableHitSchema)
    }
  }

  /** The rows of required ids when no search ran, every hit column NULL. */
  private def missing(
      spark: SparkSession,
      required: Option[Array[Long]],
      schema: StructType
  ): RDD[InternalRow] = required match {
    case None => spark.sparkContext.emptyRDD[InternalRow]
    case Some(ids) =>
      val width = schema.size
      spark.sparkContext
        .parallelize(ids.toSeq, math.max(1, math.min(ids.length, 64)))
        .map { id =>
          val row = new GenericInternalRow(width)
          row.setLong(0, id)
          row: InternalRow
        }
  }

  /** One query's answer as the rows the result contract promises: rank from
    * one, best first.
    */
  private[read] def rows(query: Long, packed: Array[Byte]): Iterator[Row] = {
    // One row at a time: at k=16,384 a query's answer is 16,384 rows, and a
    // vector of them per query is what the reduce task would otherwise hold
    // on top of the packed bytes.
    val count = CandidateBytes.count(packed)
    Iterator.tabulate(count)(at =>
      Row(
        query,
        at + 1,
        CandidateBytes.score(packed, at),
        CandidateBytes.segmentId(packed, at),
        CandidateBytes.rowOffset(packed, at)
      )
    )
  }

  private def empty(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.emptyDataset[SearchResult].toDF()
  }

  private[read] def dimensionOf(field: FieldSchema): Int = field.typeParams
    .find(_.key == "dim")
    .map(_.value.toInt)
    .getOrElse(
      throw new IllegalArgumentException(
        s"Field '${field.name}' declares no dimension"
      )
    )

  /** A search runs over a float, float16, bfloat16 or int8 field, by a metric
    * that field's indexes take. Binary vector fields are not searched
    * (docs/design/README.md, decision log 2026-10-09).
    */
  private[read] def checkSearchable(
      metric: MetricType,
      layout: VectorLayout
  ): Unit = {
    require(
      layout.elementType != VectorElementType.Bit,
      "A binary vector field is not searched; the connector searches FloatVector, Float16Vector, BFloat16Vector and Int8Vector fields"
    )
    val supported = MetricType.forElementType(layout.elementType)
    require(
      supported.contains(metric),
      s"A ${layout.elementType} field takes ${supported
          .map(_.name)
          .sorted
          .mkString(" or ")}, not $metric"
    )
  }

}

/** The columns every search returns, before the output columns. */
private[read] case class SearchResult(
    query_id: Long,
    rank: Int,
    _score: Double,
    _segment_id: Long,
    _row_offset: Long
)
