package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.zilliz.milvus.storage.expr.{Expr, PredicateExpr}
import com.zilliz.milvus.storage.index.{
  CandidateBytes,
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
  * shares its delivery of the query set (`SearchQueries`) and its merge stage
  * (`SearchMerge`).
  */
object MilvusSearch extends Logging {

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
  ): SearchMerge.Hits = {
    val limits = SearchLimits.from(options)
    val tasks = partitions.map(_.task)
    if (searchMode == SearchMode.Index) {
      SegmentIndexHandle.check(tasks, field.fieldID, searchMetric)
    }
    val queryCount = queries.count
    val hitSchema =
      if (required.isEmpty) SearchMerge.HitSchema
      else SearchMerge.NullableHitSchema
    val resultSchema = StructType(hitSchema.fields ++ outputSchema.fields)
    // The set is what a nearest-by join kept of its query rows: rows whose
    // ranking value is NULL search nothing, and all of them may be. The
    // snapshot may hold no segment.
    if (queryCount == 0L || tasks.isEmpty)
      return SearchMerge.Hits(
        SearchMerge.missing(spark, required, resultSchema.size),
        resultSchema
      )
    require(
      queryCount <= Int.MaxValue,
      s"A search takes at most ${Int.MaxValue} queries at a time, not $queryCount"
    )

    val budget =
      SearchBudget.of(spark, limits, searchMode == SearchMode.Index, layout, k)
    val delivery =
      SearchQueries.Delivery.of(queries, layout, limits.queriesMaxBytes)
    val caseInsensitive = new CaseInsensitiveStringMap(options.asJava)
    val read = MilvusOption(caseInsensitive).readLimits
    // A block the call named is used as it is; otherwise each executor
    // chooses one for its own machine when it opens a segment.
    val namedBatch =
      if (caseInsensitive.containsKey(MilvusOption.ReadBatchMaxBytes))
        Some(read.batchMaxBytes)
      else None
    val plan = SearchPlan.of(
      tasks,
      layout,
      budget.concurrency,
      queryCount.toInt,
      k,
      budget.group.bytes,
      SearchPlan.Budget(budget.segments.bytes, budget.queries.bytes),
      footprint(
        searchMode,
        field,
        searchMetric,
        layout,
        SearchResources.exactScanBatch(namedBatch).bytes
      ),
      delivery.shuffled,
      // The batched distance entry is single-threaded on its task, so an
      // exact search is as parallel as its tasks (decision 28).
      splitQueries = searchMode == SearchMode.Exact,
      rangesWanted = limits.queryRanges
    )
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
      read.arrowMaxBytes,
      budget.slots,
      namedBatch,
      plan.resident,
      // Between two Knowhere calls a search task checks, collects and packs
      // its candidates on as many threads as it holds cores, unless the call
      // said.
      if (limits.collectThreads > 0) limits.collectThreads else budget.cores,
      predicate,
      Some(function),
      limits.callsInFlight
    )
    // What the first stage has to get through, so its progress has a
    // denominator. An index probe visits part of a segment and Knowhere does
    // not say how much, so index mode has no pair total and counts segment
    // searches instead.
    val comparedPairsTotal =
      if (searchMode != SearchMode.Exact) 0L
      else SearchProgress.comparedPairs(queryCount, tasks)
    logPlan(
      budget,
      plan,
      spec,
      queries,
      delivery,
      tasks.size,
      comparedPairsTotal
    )

    // The merge and take stages run under Spark's own task slots, so a task's
    // share of the unmanaged heap is the queries' budget at that concurrency.
    val stageHeap = budget.heapFor(budget.resources.tasksPerExecutor)
    val mergeParts =
      SearchMerge.partitions(spark, queryCount, plan.tasks, k, stageHeap.bytes)
    logInfo(
      s"Merge stage: $queryCount queries x ${plan.tasks} answers of $k over $mergeParts partitions, " +
        s"about ${queryCount / mergeParts} queries a partition at ${plan.tasks.toLong * k * CandidateBytes.Width} bytes each " +
        s"against ${stageHeap.bytes} bytes of heap a task"
    )
    val found = candidates(
      spark,
      delivery,
      partitions,
      plan,
      spec,
      layout,
      metrics,
      budget
    )
    SearchProgress.watch(
      spark.sparkContext,
      found,
      metrics,
      comparedPairsTotal,
      index = searchMode == SearchMode.Index,
      queryGroups = plan.groups.size,
      segments = plan.sets.map(_.size).sum
    )
    val hits =
      SearchMerge.merged(spark, found, k, searchMetric, mergeParts, required)
    if (outputSchema.isEmpty)
      SearchMerge.Hits(hits.queryExecution.toRdd, hits.schema)
    else
      take(
        spark,
        hits,
        outputSchema,
        partitions,
        spec.arrowMaxBytes,
        metrics,
        queryCount * k,
        stageHeap
      )
  }

  /** How many first-stage tasks run at once, and what one of them may keep
    * (docs/design/architecture/vector-search.html section 2.1,
    * search-resources.html section 3.3). An index search declares that its
    * tasks take every core of their executor, so one runs per executor;
    * otherwise a task takes `spark.task.cpus`.
    *
    * @param profile
    *   the task resources the first stage declares, when it declares them
    * @param slots
    *   the first-stage tasks one executor runs at once
    * @param cores
    *   the cores one of them holds
    * @param segments
    *   what one of them may keep off the heap
    * @param queries
    *   what one of them may keep on the heap
    * @param group
    *   what one Knowhere call answers: enough queries for the cores the task
    *   holds, within the heap the task has (search-resources.html 3.3; the
    *   explicit option is used as it is)
    */
  private final case class SearchBudget(
      resources: TaskResources,
      profile: Option[ResourceProfile],
      slots: Int,
      cores: Int,
      heap: Long,
      fraction: Double,
      segments: SearchResources.Choice,
      queries: SearchResources.Choice,
      group: SearchResources.Choice
  ) {

    /** The first-stage tasks the whole search runs at once. */
    def concurrency: Int = resources.executors * slots

    /** The unmanaged heap of one task when `tasks` run at once on an executor.
      */
    def heapFor(tasks: Int): SearchResources.Choice =
      SearchResources.queryBudget(heap, fraction, tasks)
  }

  private object SearchBudget {

    def of(
        spark: SparkSession,
        limits: SearchLimits,
        index: Boolean,
        layout: VectorLayout,
        k: Int
    ): SearchBudget = {
      val resources = TaskResources.of(spark)
      val profile = if (index) resources.wholeExecutor else None
      val slots = if (profile.nonEmpty) 1 else resources.tasksPerExecutor
      val cores =
        if (profile.nonEmpty) resources.executorCores
        else math.max(1, resources.taskCpus)
      val (memoryLimit, heap) = SearchResources.executorMemory(spark)
      val fraction = SearchResources.memoryFraction(spark)
      val queries = SearchResources.queryBudget(heap, fraction, slots)
      SearchBudget(
        resources,
        profile,
        slots,
        cores,
        heap,
        fraction,
        SearchResources.segmentBudget(
          memoryLimit,
          heap,
          slots,
          limits.segmentsMaxBytes,
          index = index
        ),
        queries,
        SearchResources.groupBytes(
          limits.groupMaxBytes,
          queries.bytes,
          cores,
          SearchPlan.bytesPerQuery(layout, k)
        )
      )
    }
  }

  /** What a segment costs the task that searches it: the index the snapshot
    * recorded, as Knowhere holds it once loaded, or the vectors an exact scan
    * reads `blockBytes` at a time.
    */
  private def footprint(
      searchMode: SearchMode,
      field: FieldSchema,
      metric: MetricType,
      layout: VectorLayout,
      blockBytes: Long
  ): SegmentReadTask => SearchPlan.Footprint = task =>
    (if (searchMode == SearchMode.Index)
       SegmentIndexHandle.select(task, field.fieldID, metric)
     else None) match {
      case Some(index) if index.serializedSize > 0L =>
        SearchPlan.Footprint.index(index.serializedSize)
      case Some(_) => SearchPlan.Footprint.unsizedIndex
      case None    => SearchPlan.Footprint.vectors(task, layout, blockBytes)
    }

  /** The two lines a planned search leaves in the driver log: the budgets and
    * how the plan stands against them, then the plan. A native allocation that
    * fails does not surface as a Java exception, so these lines are what there
    * is to read afterwards (search-resources.html sections 3.3, 3.4).
    */
  private def logPlan(
      budget: SearchBudget,
      plan: SearchPlan.Plan,
      spec: SegmentSetSearch.Spec,
      queries: SearchQueries.Input,
      delivery: SearchQueries.Delivery,
      segments: Int,
      comparedPairsTotal: Long
  ): Unit = {
    val needs = plan.needs
      .map(need =>
        s"queries kept needs heap=${need.queriesHeap} offheap=${need.queriesOffHeap}, " +
          s"segments kept needs offheap=${need.segmentsOffHeap}"
      )
      .getOrElse("no segments")
    val over = plan.kept.count(_.exists(_ > plan.capacity))
    logInfo(
      s"Search budget: ${budget.resources.describe}; ${budget.concurrency} tasks at once " +
        s"(${budget.slots} per executor${if (budget.profile.nonEmpty) ", each taking every core"
          else ""}); " +
        s"segments ${budget.segments.reason}; queries ${budget.queries.reason}; group ${budget.group.reason}; " +
        s"collectThreads=${spec.collectThreads} callsInFlight=${spec.callsInFlight}; $needs; " +
        s"kept=${plan.resident}, sets=${plan.sets.size}" +
        (if (plan.resident == SearchPlan.Resident.Segments)
           s", capacity=${plan.capacity}, " +
             (if (over == 0) "no set known to stream"
              else s"$over sets stream once per query group")
         else "") +
        s", ${plan.estimated.size} segments estimated at half the budget"
    )
    logInfo(
      s"Search plan: mode=${spec.mode}, metric=${spec.metric}, topK=${spec.k}, " +
        s"queries=${queries.count}, queryBytes=${SearchQueries
            .bytes(queries.count, spec.layout)} " +
        s"delivered by ${delivery.describe}, " +
        s"queryGroups=${plan.groups.size}, segments=$segments in " +
        s"${plan.sets.size} sets x ${plan.queryRanges.size} query ranges = " +
        s"${plan.tasks} tasks, collectThreads=${spec.collectThreads}, " +
        s"work=${plan.groups.size.toLong * plan.sets.map(_.size.toLong).sum} segment searches" +
        (if (comparedPairsTotal > 0L)
           s" over $comparedPairsTotal distance pairs"
         else "")
    )
  }

  /** The take stage: the columns of `outputSchema` read for the hits alone.
    * What a take task buffers is every hit of its partition, `hitCount` over
    * the tasks, against the same heap share as the merge.
    */
  private def take(
      spark: SparkSession,
      hits: DataFrame,
      outputSchema: StructType,
      partitions: Seq[MilvusInputPartition],
      arrowMaxBytes: Long,
      metrics: SearchMetrics,
      hitCount: Long,
      stageHeap: SearchResources.Choice
  ): SearchMerge.Hits = {
    val partitioning = SearchTake.partitioning(
      hitCount,
      math.max(1, partitions.size),
      stageHeap.bytes,
      spark.sessionState.conf.numShufflePartitions
    )
    logInfo(
      s"Take stage: $hitCount hits over ${partitioning.partitions} tasks" +
        s" (${partitioning.buckets} bucket(s) per segment), " +
        s"about ${hitCount / partitioning.partitions} hits a task at " +
        s"${SearchTake.HitRowBytes} bytes each against ${stageHeap.bytes} bytes of heap a task"
    )
    SearchMerge.Hits(
      SearchTake.rows(
        hits,
        outputSchema,
        partitions
          .map(partition => partition.task.segmentId -> partition)
          .toMap,
        arrowMaxBytes,
        metrics,
        partitioning
      ),
      StructType(hits.schema.fields ++ outputSchema.fields)
    )
  }

  /** The candidates of the first stage: each task searches one segment set over
    * one query range, reading the groups of its range one at a time
    * (docs/design/architecture/vector-search.html section 2.1).
    */
  private def candidates(
      spark: SparkSession,
      delivery: SearchQueries.Delivery,
      partitions: Seq[MilvusInputPartition],
      plan: SearchPlan.Plan,
      spec: SegmentSetSearch.Spec,
      layout: VectorLayout,
      metrics: SearchMetrics,
      budget: SearchBudget
  ): RDD[(Long, Array[Byte])] = {
    val byId =
      partitions.map(partition => partition.task.segmentId -> partition).toMap
    val sets = plan.sets.map(_.map(task => byId(task.segmentId)))
    val groups = plan.groups
    val delivered = delivery.groups(
      spark,
      groups,
      spec.metric,
      layout,
      budget.resources,
      budget.heapFor(1).bytes
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
    val kept = plan.kept.toVector
    val held = spark.sparkContext.broadcast(sets.toVector)
    val ranges = plan.queryRanges
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
    budget.profile.fold(searched)(searched.withResources)
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
