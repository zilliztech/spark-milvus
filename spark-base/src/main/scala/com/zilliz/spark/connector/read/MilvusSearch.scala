package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.HashPartitioner

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
  * shares the merge stage kept here.
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
    val limits = SearchLimits.from(options)
    val tasks = partitions.map(_.task)
    if (searchMode == SearchMode.Index) {
      SegmentIndexHandle.check(tasks, field.fieldID, searchMetric)
    }
    val queryCount = queries.count
    val hitSchema = if (required.isEmpty) HitSchema else NullableHitSchema
    val resultSchema = StructType(hitSchema.fields ++ outputSchema.fields)
    // The set is what a nearest-by join kept of its query rows: rows whose
    // ranking value is NULL search nothing, and all of them may be. The
    // snapshot may hold no segment.
    if (queryCount == 0L || tasks.isEmpty)
      return Hits(missing(spark, required, resultSchema.size), resultSchema)
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
      mergePartitions(spark, queryCount, plan.tasks, k, stageHeap.bytes)
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
    val hits = merged(spark, found, k, searchMetric, mergeParts, required)
    if (outputSchema.isEmpty) Hits(hits.queryExecution.toRdd, hits.schema)
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
  ): Hits = {
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
    Hits(
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

  /** Every query's candidates become its global top-k, best first, as rows of
    * [[HitSchema]]; with `required`, an id that found nothing has a row whose
    * hit columns are NULL.
    */
  private[read] def merged(
      spark: SparkSession,
      candidates: RDD[(Long, Array[Byte])],
      k: Int,
      metric: MetricType,
      partitions: Int,
      required: Option[Array[Long]] = None
  ): DataFrame =
    spark.createDataFrame(
      mergedByQuery[Row](
        spark,
        candidates,
        partitions,
        CandidateBytes.merge(_, _, k, metric),
        rows,
        required,
        id => Row(id, null, null, null, null)
      ),
      if (required.isEmpty) HitSchema else NullableHitSchema
    )

  /** The merge stage of either input: every query's answers merged into one,
    * the rows `rowsOf` makes of it, and with `required` the `absent` row of
    * each id that found nothing.
    *
    * Each task already merged its own segments, so what arrives here is one
    * packed answer per (query, task) and the merge is a walk over two sorted
    * lists, not an aggregation over candidates. Map-side combining is off on
    * purpose: a task emits each of its queries once, so combining on the map
    * side would hold every query's answer until the task ends instead of
    * writing each one as it is packed (section 2.6).
    */
  private[read] def mergedByQuery[A: ClassTag](
      spark: SparkSession,
      answers: RDD[(Long, Array[Byte])],
      partitions: Int,
      merge: (Array[Byte], Array[Byte]) => Array[Byte],
      rowsOf: (Long, Array[Byte]) => Iterator[A],
      required: Option[Array[Long]],
      absent: Long => A
  ): RDD[A] = {
    require(partitions > 0, s"The merge needs a partition: $partitions")
    val partitioner = new HashPartitioner(partitions)
    val combined = answers.combineByKeyWithClassTag[Array[Byte]](
      (packed: Array[Byte]) => packed,
      merge,
      merge,
      partitioner,
      mapSideCombine = false
    )
    required match {
      case None =>
        combined.flatMap { case (query, packed) => rowsOf(query, packed) }
      case Some(ids) =>
        // A required id that no task found anything for has no key here; the
        // partition its key would hash to says so after its own answers.
        val wanted = spark.sparkContext.broadcast(ids)
        combined.mapPartitionsWithIndex { (index, merged) =>
          val found = new java.util.HashSet[java.lang.Long]()
          merged.flatMap { case (query, packed) =>
            found.add(query)
            rowsOf(query, packed)
          } ++ wanted.value.iterator
            .filter(id =>
              partitioner.getPartition(id) == index && !found.contains(id)
            )
            .map(absent)
        }
    }
  }

  /** The rows of required ids when no search ran: each the id, then NULLs to
    * make `width` columns.
    */
  private[read] def missing(
      spark: SparkSession,
      required: Option[Array[Long]],
      width: Int
  ): RDD[InternalRow] = required match {
    case None => spark.sparkContext.emptyRDD[InternalRow]
    case Some(ids) =>
      spark.sparkContext
        .parallelize(ids.toSeq, math.max(1, math.min(ids.length, 64)))
        .map(absentRow(_, width))
  }

  /** A required id's row without a hit: the id, then NULLs to make `width`
    * columns.
    */
  private[read] def absentRow(id: Long, width: Int): InternalRow = {
    val row = new GenericInternalRow(width)
    row.setLong(0, id)
    row
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
