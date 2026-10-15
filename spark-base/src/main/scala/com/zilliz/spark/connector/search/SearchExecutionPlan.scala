package com.zilliz.spark.connector.search

import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.read.exec.SegmentIndexHandle
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.{MetricType, VectorLayout}
import com.zilliz.milvus.storage.vector.{
  CandidateBytes,
  IndexSearch,
  SearchPlan
}
import com.zilliz.spark.connector.options.{SearchLimits, TaskResources}

/** Driver-side execution choices shared by Milvus and DataFrame searches.
  * Resource formulas live in [[SearchResources]] and the format-neutral set and
  * range algorithm lives in [[SearchPlan]]. Tasks receive only [[Task]], never
  * a driver-side ResourceProfile or a SparkSession.
  *
  * Metadata determines whether to attempt retention; the executor must still
  * measure loaded bytes and stream a set that exceeds its planned capacity.
  */
private[connector] object SearchExecutionPlan {

  sealed trait Retention extends Serializable
  object Retention {
    case object Queries extends Retention
    case object Segments extends Retention
    final case class Stream(plannedBytes: Long) extends Retention
  }

  /** The complete execution policy for one segment set and query range. */
  final case class Task(
      retention: Retention,
      keptMaxBytes: Long,
      batchMaxBytes: Long,
      collectThreads: Int,
      callsInFlight: Int,
      prefetchIndexes: Boolean
  ) extends Serializable

  final case class Milvus(
      budget: Budget,
      segments: SearchPlan.Plan,
      delivery: SearchQueries.Delivery,
      packingProfile: Option[ResourceProfile],
      tasks: Vector[Task],
      batch: SearchResources.Choice,
      stageHeap: SearchResources.Choice,
      mergePartitions: Int,
      takePartitioning: Option[SearchTake.Partitioning]
  )

  final case class Frame(
      groups: Seq[SearchPlan.QueryGroup],
      delivery: SearchQueries.Delivery,
      packingProfile: Option[ResourceProfile],
      blockRows: Int,
      blockMaxBytes: Long,
      retainedMaxBytes: Long,
      tasks: Int,
      mergePartitions: Int
  )

  def milvus(
      spark: SparkSession,
      limits: SearchLimits,
      queries: SearchQueries.Input,
      tasks: Seq[SegmentReadTask],
      fieldId: Long,
      layout: VectorLayout,
      k: Int,
      metric: MetricType,
      mode: SearchMode,
      configuredBatch: Option[Long],
      takeColumns: Boolean
  ): Milvus = {
    val index = mode == SearchMode.PreferIndex
    val budget = Budget.of(spark, limits, index, layout, k)
    val delivery = queryDelivery(queries, layout, limits.queriesMaxBytes)
    val batch = SearchResources.exactScanBatch(configuredBatch)
    val descriptors =
      tasks.map(task => SearchPlan.Segment(task.segmentId, task.knownRows))
    val selected = tasks.map { task =>
      task.segmentId -> (if (index)
                           SegmentIndexHandle.select(task, fieldId, metric)
                         else None)
    }.toMap
    val plan = SearchPlan.of(
      descriptors,
      layout,
      budget.concurrency,
      queries.count.toInt,
      k,
      budget.group.bytes,
      SearchPlan.Budget(budget.segments.bytes, budget.queries.bytes),
      segment =>
        selected(segment.segmentId) match {
          case Some(descriptor) if descriptor.serializedSize > 0L =>
            SearchPlan.Footprint.index(descriptor.serializedSize)
          case Some(_) => SearchPlan.Footprint.unsizedIndex
          case None =>
            SearchPlan.Footprint.vectors(segment, layout, batch.bytes)
        },
      delivery.shuffled,
      splitQueries = mode == SearchMode.Exact,
      rangesWanted = limits.queryRanges,
      workingBytes = groupQueries =>
        if (index)
          IndexSearch.resultBytes(groupQueries, k, limits.callsInFlight)
        else 0L
    )
    val threads =
      if (limits.collectThreads > 0) limits.collectThreads else budget.cores
    val policies = plan.sets.zipWithIndex.flatMap { case (set, setIndex) =>
      plan.queryRanges.map { range =>
        val keep = retention(
          plan.resident,
          range.size,
          plan.kept.lift(setIndex).flatten,
          plan.capacity
        )
        Task(
          keep,
          plan.capacity,
          batch.bytes,
          threads,
          limits.callsInFlight,
          keep == Retention.Queries && prefetchIndexes(
            index,
            set.map(segment =>
              selected(segment.segmentId).map(_.serializedSize)
            ),
            budget.segments.bytes,
            range
              .map(i => plan.groups(i).queries.toLong)
              .sum * layout.rowBytes.toLong,
            IndexSearch.resultBytes(
              range.map(i => plan.groups(i).queries).max,
              k,
              1
            )
          )
        )
      }
    }.toVector
    val stageHeap = budget.heapFor(budget.resources.tasksPerExecutor)
    Milvus(
      budget,
      plan,
      delivery,
      if (delivery.shuffled)
        packingProfile(
          plan.groups,
          layout,
          budget.resources,
          budget.heapFor(1).bytes
        )
      else None,
      policies,
      batch,
      stageHeap,
      mergePartitions(
        spark,
        queries.count,
        plan.tasks,
        plan.sets.size,
        k,
        stageHeap.bytes
      ),
      if (takeColumns)
        Some(
          takePartitioning(
            queries.count * k,
            math.max(1, tasks.size),
            stageHeap.bytes,
            spark.sessionState.conf.numShufflePartitions
          )
        )
      else None
    )
  }

  /** A frame task carries its candidate rows and scans one base partition
    * against one query group. Its group is sized for one core, as before.
    */
  def frame(
      spark: SparkSession,
      limits: SearchLimits,
      queries: SearchQueries.Input,
      layout: VectorLayout,
      k: Int,
      rowBytes: Long,
      basePartitions: Int
  ): Frame = {
    val (_, heap) = SearchResources.executorMemory(spark)
    val fraction = SearchResources.memoryFraction(spark)
    val resources = TaskResources.of(spark)
    val taskHeap =
      SearchResources.queryBudget(heap, fraction, resources.tasksPerExecutor)
    val blockMaxBytes = math.max(
      1L,
      math.min(SearchResources.exactScanBatch(None).bytes, taskHeap.bytes / 4L)
    )
    val availableHeapBytes = math.max(0L, taskHeap.bytes - blockMaxBytes)
    val group = SearchResources.groupBytes(
      limits.groupMaxBytes,
      taskHeap.bytes,
      1,
      Math.addExact(
        frameQueryBytes(layout, k),
        Math.multiplyExact(k.toLong, rowBytes)
      )
    )
    val groups = frameGroups(
      queries.count.toInt,
      layout,
      k,
      rowBytes,
      group.bytes,
      availableHeapBytes
    )
    val blockRows = math
      .max(
        1L,
        SearchResources.exactScanBatch(None).bytes / layout.rowBytes.toLong
      )
      .min(MaxFrameBlockRows.toLong)
      .toInt
    val delivery = queryDelivery(queries, layout, limits.queriesMaxBytes)
    val tasks = basePartitions * groups.size
    val queryHeap =
      groups.map(_.queries.toLong).max * frameQueryBytes(layout, k)
    // A retained row also appears in the packed answer during encoding.
    val retainedMaxBytes =
      math.max(0L, (availableHeapBytes - queryHeap) / 2L)
    Frame(
      groups,
      delivery,
      if (delivery.shuffled)
        packingProfile(
          groups,
          layout,
          resources,
          SearchResources.queryBudget(heap, fraction, 1).bytes
        )
      else None,
      blockRows,
      blockMaxBytes,
      retainedMaxBytes,
      tasks,
      mergePartitions(
        spark,
        queries.count,
        math.max(1, tasks),
        math.max(1, basePartitions),
        k,
        taskHeap.bytes,
        CandidateBytes.Width.toLong + 4L + rowBytes
      )
    )
  }

  /** One group's queries always stay, even when the whole search was planned
    * with resident segments. Unknown or estimated sizes still attempt holding;
    * only actual loaded bytes can decide whether that attempt succeeds.
    */
  def retention(
      resident: SearchPlan.Resident,
      groupCount: Int,
      plannedBytes: Option[Long],
      capacity: Long
  ): Retention =
    if (groupCount == 1 || resident == SearchPlan.Resident.Queries)
      Retention.Queries
    else
      plannedBytes match {
        case Some(bytes) if bytes > capacity => Retention.Stream(bytes)
        case _                               => Retention.Segments
      }

  /** Every segment has a sized index, and remaining task off-heap memory covers
    * two kept indexes plus one loading footprint.
    */
  def prefetchIndexes(
      index: Boolean,
      sizes: Seq[Option[Long]],
      offHeapBytes: Long,
      queryBytes: Long,
      resultBytes: Long
  ): Boolean =
    index && sizes.size > 1 && sizes.forall(_.exists(_ > 0L)) && {
      val copies =
        2L * SearchPlan.Footprint.IndexKeptCopies + SearchPlan.Footprint.IndexLoadingCopies
      copies * sizes.flatten.max <= offHeapBytes - queryBytes - resultBytes
    }

  def queryDelivery(
      queries: SearchQueries.Input,
      layout: VectorLayout,
      maxBytes: Long
  ): SearchQueries.Delivery =
    queries match {
      case packed: SearchQueries.Packed => packed
      case SearchQueries.Rows(selected, count) =>
        if (SearchQueries.bytes(count, layout) > maxBytes)
          SearchQueries.Shuffled(selected)
        else SearchQueries.Broadcast(selected)
    }

  /** A frame block holds copied output rows and uses one shared vector buffer.
    */
  private val MaxFrameBlockRows = 65536

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
  final case class Budget(
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

  object Budget {

    def of(
        spark: SparkSession,
        limits: SearchLimits,
        index: Boolean,
        layout: VectorLayout,
        k: Int
    ): Budget = {
      val resources = TaskResources.of(spark)
      val profile = if (index) resources.wholeExecutor else None
      val slots = if (profile.nonEmpty) 1 else resources.tasksPerExecutor
      val cores =
        if (profile.nonEmpty) resources.executorCores
        else math.max(1, resources.taskCpus)
      val (memoryLimit, heap) = SearchResources.executorMemory(spark)
      val fraction = SearchResources.memoryFraction(spark)
      val queries = SearchResources.queryBudget(heap, fraction, slots)
      Budget(
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

  /** Query matrix, ids and bounded top-k state for one frame query. */
  private def frameQueryBytes(layout: VectorLayout, k: Int): Long =
    SearchPlan.bytesPerQuery(layout, k) + SearchPlan.QueryIdBytes +
      SearchPlan.TopKQueueBytes

  /** Query groups of equal size, the last one shorter, bounded by the group
    * limit and the heap left after reserving the scan block. The retained rows
    * and the one answer being encoded each fit half the remaining heap.
    *
    * One query remains indivisible when its maximum answer would not fit: fewer
    * than k rows may exist, so the executor checks the actual bytes.
    */
  private[connector] def frameGroups(
      queries: Int,
      layout: VectorLayout,
      k: Int,
      rowBytes: Long,
      groupMaxBytes: Long,
      availableHeapBytes: Long
  ): Seq[SearchPlan.QueryGroup] = {
    require(queries > 0, s"A frame search needs queries: $queries")
    require(rowBytes >= 0L, s"A frame row has negative bytes: $rowBytes")
    require(
      availableHeapBytes >= 0L,
      s"A frame has negative heap: $availableHeapBytes"
    )
    val queryHeap = frameQueryBytes(layout, k)
    val retainedRows = Math.multiplyExact(k.toLong, rowBytes)
    val answer = Math.multiplyExact(
      k.toLong,
      Math.addExact(CandidateBytes.Width.toLong + 4L, rowBytes)
    )
    val perQuery = Math.addExact(queryHeap, retainedRows)
    // Both regions use the same half-heap limit. Only one query's encoded
    // answer coexists with the group's retained rows, not every answer.
    val byRows =
      if (
        queryHeap > availableHeapBytes ||
        retainedRows > (availableHeapBytes - queryHeap) / 2L
      ) 0L
      else availableHeapBytes / (queryHeap + 2L * retainedRows)
    val byAnswer =
      if (answer > availableHeapBytes / 2L) 0L
      else (availableHeapBytes - 2L * answer) / queryHeap
    val bounded = math.min(groupMaxBytes / perQuery, math.min(byRows, byAnswer))
    val size = math.max(1L, bounded).min(queries.toLong).toInt
    (0 until queries by size).map(first =>
      SearchPlan.QueryGroup(first, math.min(size, queries - first))
    )
  }

  /** The task resources of the stage that packs the query groups for the
    * shuffle. A packing task holds one group's ids and vectors on the heap,
    * outside what Spark manages, so no more of them run at once on an executor
    * than that heap holds with room to spare.
    */
  def packingProfile(
      groups: Seq[SearchPlan.QueryGroup],
      layout: VectorLayout,
      resources: TaskResources,
      executorQueryHeapBytes: Long
  ): Option[ResourceProfile] = {
    val perGroup = groups.map(_.queries.toLong).max *
      (SearchPlan.QueryIdBytes + layout.rowBytes.toLong)
    val fits =
      math.max(1L, executorQueryHeapBytes / (perGroup + perGroup / 2))
    resources.atMost(math.min(fits, Int.MaxValue.toLong).toInt)
  }

  /** How many queries one merge partition is meant to take. The merge walks
    * sorted lists, so a partition's work is linear in the candidates it reads;
    * this only keeps a partition's reduce-side map from holding more than a few
    * thousand queries' answers at once on a cluster whose parallelism is far
    * below its query count.
    */
  val QueriesPerMergePartition: Int = 4096

  /** How many partitions the merge runs in: one for each task slot the job has,
    * and never more than there are queries. The merge is an RDD shuffle rather
    * than a SQL aggregation, so nothing downstream re-partitions it and
    * `spark.sql.shuffle.partitions` does not apply.
    */
  def mergePartitions(
      spark: SparkSession,
      queries: Long,
      firstStageTasks: Int,
      candidateSourcesPerQuery: Int,
      k: Int,
      heapBytesPerTask: Long,
      candidateBytes: Long = CandidateBytes.Width.toLong
  ): Int = {
    require(queries > 0, s"A search needs at least one query: $queries")
    require(
      firstStageTasks > 0,
      s"A search needs at least one task: $firstStageTasks"
    )
    require(
      candidateSourcesPerQuery > 0,
      "Every query needs at least one candidate source"
    )
    require(k > 0, s"A search needs a positive k: $k")
    // At least one partition for every task slot the job has and for every
    // first-stage task, so no executor sits out the merge; more when the
    // queries would otherwise pile up in one partition; never more than there
    // are queries, since a query is one key and cannot be split.
    //
    // How many queries a partition may hold is the smaller of a fixed few
    // thousand and what the task's heap fits: every query arrives as one
    // packed answer per candidate source (segment set or base partition), and the
    // reduce-side map holds them until they are merged. 100k queries at
    // k=16,384 over 15 tasks were 5.9 MB a query; 64 partitions of 1,563
    // queries each ran the 6 GiB heap out (2026-09-26 decision). Half the
    // heap share is left for the merged answers and the rows they become.
    val slots = math.max(1, spark.sparkContext.defaultParallelism)
    val bytesPerQuery =
      candidateSourcesPerQuery.toLong * k.toLong * candidateBytes
    val byHeap =
      math.max(1L, math.max(0L, heapBytesPerTask) / 2L / bytesPerQuery)
    val perPartition = math.min(QueriesPerMergePartition.toLong, byHeap)
    val byQueries = (queries + perPartition - 1L) / perPartition
    val wanted =
      math.max(math.max(slots.toLong, firstStageTasks.toLong), byQueries)
    math.max(1L, math.min(queries, wanted)).toInt
  }

  /** The partitioning that keeps a task's buffered hits inside
    * `heapBytesPerTask`, at half that to leave room for hash skew: never fewer
    * partitions than `minimum` (the session's shuffle partitions, so a small
    * search keeps its shape), never more than a bucket per hit.
    */
  def takePartitioning(
      hits: Long,
      segments: Int,
      heapBytesPerTask: Long,
      minimum: Int
  ): SearchTake.Partitioning = {
    require(
      hits >= 0L && segments > 0 && minimum > 0,
      s"$hits hits, $segments segments, $minimum minimum"
    )
    val rowsPerTask =
      math.max(1L, math.max(0L, heapBytesPerTask) / SearchTake.HitRowBytes / 2L)
    val needed = (hits + rowsPerTask - 1L) / rowsPerTask
    val partitions =
      math.max(minimum.toLong, math.min(needed, Int.MaxValue.toLong)).toInt
    val buckets =
      math.max(1L, (partitions.toLong + segments - 1L) / segments).toInt
    SearchTake.Partitioning(partitions, buckets)
  }

}
