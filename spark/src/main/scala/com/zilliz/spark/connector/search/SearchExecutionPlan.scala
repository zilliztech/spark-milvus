package com.zilliz.spark.connector.search

import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.read.exec.SegmentIndexHandle
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.{MetricType, VectorLayout}
import com.zilliz.milvus.storage.vector.{
  CandidateBytes,
  CandidateDictionary,
  CandidateRuns,
  CarriedCandidates,
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
      prefetchIndexes: Boolean,
      pipelineGroups: Boolean
  ) extends Serializable

  /** How the merge stage runs on one executor: `running` of its tasks at once,
    * each declaring `cores` so that no more fit; `maxRuns` answers a query
    * retains before compacting; `queriesPerPartition` queries a partition is
    * cut for, in `partitions` partitions. `heap` is the unmanaged heap one task
    * then has for its own objects and `executionHeap` its share of the pool
    * Spark manages, where the reduce-side map holds the retained runs.
    * `profile` is the declaration, None when the default task slots already run
    * that many or this master takes none.
    */
  final case class MergeStage(
      running: Int,
      cores: Int,
      maxRuns: Int,
      queriesPerPartition: Int,
      partitions: Int,
      heap: SearchResources.Choice,
      executionHeap: SearchResources.Choice,
      profile: Option[ResourceProfile],
      footprint: SearchMerge.Footprint
  )

  /** @param stageHeap
    *   the unmanaged heap of a task under the default task slots, which the
    *   take stage plans against
    * @param mergeStage
    *   the merge stage's concurrency and the heap its tasks plan against
    */
  /** @param segmentK
    *   the most hits one segment answers a query with: k, or its rows when
    *   fewer, which sizes the search stage
    * @param setK
    *   the most hits one segment set answers a query with, which sizes the
    *   merge
    */
  final case class Milvus(
      budget: Budget,
      segments: SearchPlan.Plan,
      delivery: SearchQueries.Delivery,
      packingProfile: Option[ResourceProfile],
      tasks: Vector[Task],
      batch: SearchResources.Choice,
      stageHeap: SearchResources.Choice,
      candidateDictionary: CandidateDictionary,
      mergeStage: MergeStage,
      merge: SearchMerge.Plan,
      takePartitioning: Option[SearchTake.Partitioning],
      segmentK: Int,
      setK: Int
  )

  final case class Frame(
      groups: Seq[SearchPlan.QueryGroup],
      delivery: SearchQueries.Delivery,
      packingProfile: Option[ResourceProfile],
      blockRows: Int,
      blockMaxBytes: Long,
      retainedMaxBytes: Long,
      tasks: Int,
      mergeStage: MergeStage,
      merge: SearchMerge.Plan
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
    // A segment answers a query with at most its rows, so the search stage is
    // sized for that many hits a query rather than a k far above it.
    val segmentK = answerK(k, tasks.flatMap(_.knownRows).reduceOption(_ max _))
    val budget =
      Budget.of(spark, limits, index, layout, segmentK, queries.count)
    val delivery = queryDelivery(queries, layout, limits.queriesMaxBytes)
    val batch = SearchResources.exactScanBatch(configuredBatch)
    val descriptors =
      tasks.map(task => SearchPlan.Segment(task.segmentId, task.knownRows))
    val selected = tasks.map { task =>
      task.segmentId -> (if (index)
                           SegmentIndexHandle.select(task, fieldId, metric)
                         else None)
    }.toMap
    val dictionary = candidateDictionary(tasks)
    val plan = SearchPlan.of(
      descriptors,
      layout,
      budget.concurrency,
      queries.count.toInt,
      segmentK,
      SearchResources.groupPlanBytes(
        budget.group.bytes,
        layout,
        segmentK,
        budget.pipelineGroups
      ),
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
          IndexSearch.resultBytes(groupQueries, segmentK, limits.callsInFlight)
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
              segmentK,
              1
            )
          ),
          pipelineGroups = budget.pipelineGroups && keep == Retention.Segments
        )
      }
    }.toVector
    val stageHeap = budget.heapFor(budget.resources.tasksPerExecutor)
    // A set's answer to a query holds at most the rows of its segments, and
    // the whole search at most every row.
    val rowsOf = tasks.map(task => task.segmentId -> task.knownRows).toMap
    val setRows = plan.sets.map(set =>
      set.foldLeft(Option(0L)) { (sum, segment) =>
        for (total <- sum; rows <- rowsOf(segment.segmentId)) yield total + rows
      }
    )
    val setK = answerK(k, setRows.flatten.reduceOption(_ max _))
    val totalK = answerK(
      k,
      tasks.foldLeft(Option(0L)) { (sum, task) =>
        for (total <- sum; rows <- task.knownRows) yield total + rows
      }
    )
    val merge = mergeStage(
      spark.sparkContext.defaultParallelism,
      budget.resources,
      budget.heap,
      budget.fraction,
      plan.sets.size,
      setK,
      CandidateRuns.Plain,
      CandidateBytes.Width.toLong,
      queries.count,
      plan.tasks,
      SearchResources.fetchToMemoryBytes(spark)
    )
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
      dictionary,
      merge,
      SearchMerge.plan(
        merge.partitions,
        queries.count,
        plan.sets.size,
        k,
        merge.executionHeap.bytes,
        CandidateRuns.Plain,
        CandidateBytes.Width.toLong,
        merge.maxRuns,
        setK
      ),
      if (takeColumns)
        Some(
          takePartitioning(
            queries.count * totalK,
            math.max(1, tasks.size),
            stageHeap.bytes,
            spark.sessionState.conf.numShufflePartitions
          )
        )
      else None,
      segmentK,
      setK
    )
  }

  /** The hits a source can answer a query with: `k`, or every row it has when
    * that is fewer. Unknown row counts leave k as it is. `IndexSearch` asks the
    * index for the same bound at run time.
    */
  def answerK(k: Int, rows: Option[Long]): Int =
    rows.filter(_ >= 0L).map(r => math.min(k.toLong, r).toInt).getOrElse(k)

  /** One global segment dictionary for every candidate source in this search.
    * Known physical extents fail early when UInt32 row addresses cannot cover
    * them. Unknown extents remain eligible; every encoded offset is checked.
    * Planning estimates and index row counts do not prove physical extents.
    */
  private[search] def candidateDictionary(
      tasks: Seq[SegmentReadTask]
  ): CandidateDictionary = {
    tasks.foreach { task =>
      task.provenPhysicalRows.foreach { rows =>
        require(
          rows <= CandidateBytes.MaxRows,
          s"Segment ${task.segmentId} has $rows physical rows, exceeding the UInt32 candidate address limit of ${CandidateBytes.MaxRows}"
        )
      }
    }
    CandidateDictionary(tasks.map(_.segmentId))
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
    val heap = SearchResources.executorMemory(spark).heap
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
    val carriedBytes = CarriedCandidates.HeaderBytes.toLong + rowBytes
    val merge = mergeStage(
      spark.sparkContext.defaultParallelism,
      resources,
      heap,
      fraction,
      math.max(1, basePartitions),
      k,
      CandidateRuns.Carried,
      carriedBytes,
      queries.count,
      math.max(1, tasks),
      SearchResources.fetchToMemoryBytes(spark)
    )
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
      merge,
      // Here every candidate carries its row.
      SearchMerge.plan(
        merge.partitions,
        queries.count,
        math.max(1, basePartitions),
        k,
        merge.executionHeap.bytes,
        CandidateRuns.Carried,
        carriedBytes,
        merge.maxRuns
      )
    )
  }

  /** The merge stage's concurrency, runs and partitioning, from where one
    * query's merge state lives. A task's retained runs are the value in the
    * reduce-side map, which Spark accounts in its execution pool, `(heap - 300
    * MiB) x spark.memory.fraction` shared by the tasks that run at once; its
    * compaction output, the answer it is deserializing and the shuffle block it
    * reads -- when the block is small enough for Spark to fetch it to memory,
    * `fetchToMemoryBytes` -- are its own objects, in the heap Spark does not
    * manage. Both pools are divided by the tasks that run at once on the
    * executor. A submission whose answers are too big for a slot's own share
    * can lower `spark.network.maxRemoteBlockSizeFetchToMem` so the blocks go
    * through disk and every slot runs.
    *
    * The stage runs as many tasks at once as both pools hold one query each
    * for, preferring the default task slots (every core busy) and, at that
    * concurrency, the most runs a query may retain before compacting (fewer
    * compactions); below the default slots it declares the cores a task takes
    * so that no more fit. A partition then takes as many queries as the two
    * shares hold at once, never more than `QueriesPerMergePartition`. When one
    * task alone on its executor cannot hold one query, planning fails here
    * rather than letting the stage run out of heap.
    *
    * A master that takes no task-resource declaration (local, or dynamic
    * allocation on) runs the default slots whatever the need; planning still
    * fails when their shares are too small.
    */
  def mergeStage(
      parallelism: Int,
      resources: TaskResources,
      heapBytes: Long,
      fraction: Double,
      sources: Int,
      k: Int,
      layout: CandidateRuns.Layout,
      candidateBytes: Long,
      queries: Long,
      firstStageTasks: Int,
      fetchToMemoryBytes: Long = SearchMerge.FetchToMemoryBytes
  ): MergeStage = {
    val slots = resources.tasksPerExecutor
    val userHeap = SearchResources.queryBudget(heapBytes, fraction, 1).bytes
    val executionHeap =
      SearchResources.executionBudget(heapBytes, fraction, 1).bytes
    def held(maxRuns: Int) =
      SearchMerge.footprint(sources, k, layout, candidateBytes, maxRuns)
    // One query in a task: its runs in the map, its temporaries and the block
    // it arrives in outside it.
    def fits(running: Int, maxRuns: Int): Boolean = {
      val footprint = held(maxRuns)
      footprint.stateBytes <= executionHeap / running &&
      footprint.temporaryBytes + SearchMerge.blockBytes(
        1L,
        footprint.answerBytes,
        fetchToMemoryBytes
      ) <= userHeap / running
    }
    // The concurrencies a stage can declare: `cores` cores a task for every
    // whole-number division of the executor, most tasks first and, for one
    // count of tasks, the most cores that still fit that many. An
    // undeclarable stage has only the default slots.
    val choices: Seq[(Int, Int)] =
      if (!resources.declarable) Seq((slots, resources.taskCpus))
      else
        (resources.executorCores to resources.taskCpus by -1)
          .map(cores => (resources.executorCores / cores, cores))
          .filter { case (running, _) => running >= 1 && running <= slots }
          // the first cores seen for a count of tasks is the most that fit
          // (Scala 2.12 has no distinctBy)
          .foldLeft(Vector.empty[(Int, Int)]) { (kept, choice) =>
            if (kept.exists(_._1 == choice._1)) kept else kept :+ choice
          }
          .sortBy(-_._1)
    val chosen = choices.iterator
      .map { case (running, cores) =>
        (
          running,
          cores,
          (SearchMerge.MaxRuns to SearchMerge.MinRuns by -1)
            .find(fits(running, _))
        )
      }
      .collectFirst { case (running, cores, Some(maxRuns)) =>
        (running, cores, maxRuns)
      }
    val (running, cores, maxRuns) = chosen.getOrElse {
      val alone = if (resources.declarable) 1 else slots
      val largest = largestMergeableK(
        userHeap / alone,
        executionHeap / alone,
        sources,
        layout,
        candidateBytes,
        fetchToMemoryBytes
      )
      val minimal = held(SearchMerge.MinRuns)
      throw new IllegalArgumentException(
        s"Merging one query's $sources answers of k=$k holds ${minimal.stateBytes} bytes " +
          s"of retained runs in the reduce-side map against an execution pool share of " +
          s"${executionHeap / alone} bytes, and ${minimal.temporaryBytes + SearchMerge
              .blockBytes(1L, minimal.answerBytes, fetchToMemoryBytes)} bytes of its own against a heap share of " +
          s"${userHeap / alone} bytes" +
          (if (resources.declarable)
             " even when one task runs alone on its executor"
           else
             s"; this master takes no task cores declaration, so $slots run at once") +
          (if (largest >= 1L)
             s". Raise spark.executor.memory, or lower k to at most $largest for these executors."
           else ". Raise spark.executor.memory: no k fits these executors.")
      )
    }
    val footprint = held(maxRuns)
    val heap = SearchResources.queryBudget(heapBytes, fraction, running)
    val execution =
      SearchResources.executionBudget(heapBytes, fraction, running)
    // Queries a partition: as many as three quarters of the map share hold
    // runs for -- hash skew puts more than the expected queries in some
    // partitions, and a full share spilled a few of them on the bench -- and
    // the task's own share holds the block of, within the fixed few thousand.
    val byState =
      (execution.bytes / 4L * 3L) / math.max(1L, footprint.stateBytes)
    val ownRoom = math.max(0L, heap.bytes - footprint.temporaryBytes)
    val byBlock =
      if (ownRoom >= math.max(0L, fetchToMemoryBytes)) Long.MaxValue
      else ownRoom / math.max(1L, footprint.answerBytes)
    val perPartition = math
      .max(
        1L,
        math.min(QueriesPerMergePartition.toLong, math.min(byState, byBlock))
      )
      .toInt
    val partitions =
      mergePartitions(parallelism, queries, firstStageTasks, perPartition)
    val profile =
      if (running >= slots) None else resources.taking(cores)
    MergeStage(
      running,
      cores,
      maxRuns,
      perPartition,
      partitions,
      heap,
      execution,
      profile,
      footprint
    )
  }

  /** The largest k whose merge one task holds with `userHeapBytes` of its own
    * and `executionHeapBytes` of the pool Spark manages, 0 when none does.
    * Planned bytes grow with k, so a binary search finds it.
    */
  def largestMergeableK(
      userHeapBytes: Long,
      executionHeapBytes: Long,
      sources: Int,
      layout: CandidateRuns.Layout,
      candidateBytes: Long,
      fetchToMemoryBytes: Long = SearchMerge.FetchToMemoryBytes
  ): Long = {
    def fits(k: Int): Boolean = {
      val footprint = SearchMerge
        .footprint(sources, k, layout, candidateBytes, SearchMerge.MinRuns)
      footprint.stateBytes <= executionHeapBytes &&
      footprint.temporaryBytes + SearchMerge.blockBytes(
        1L,
        footprint.answerBytes,
        fetchToMemoryBytes
      ) <= userHeapBytes
    }
    if (!fits(1)) return 0L
    var low = 1
    var high = Int.MaxValue
    while (low < high) {
      val mid = low + (high - low + 1) / 2
      if (fits(mid)) low = mid else high = mid - 1
    }
    low.toLong
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

  /** How many first-stage tasks run at once, and what one of them may keep. An
    * index search declares that its tasks take every core of their executor, so
    * one runs per executor; otherwise a task takes `spark.task.cpus`.
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
      group: SearchResources.Choice,
      pipelineGroups: Boolean
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
        k: Int,
        queryCount: Long
    ): Budget = {
      val resources = TaskResources.of(spark)
      val profile = if (index) resources.wholeExecutor else None
      val slots = if (profile.nonEmpty) 1 else resources.tasksPerExecutor
      val cores =
        if (profile.nonEmpty) resources.executorCores
        else math.max(1, resources.taskCpus)
      val memory = SearchResources.executorMemory(spark)
      val heap = memory.heap
      val fraction = SearchResources.memoryFraction(spark)
      val queries = SearchResources.queryBudget(heap, fraction, slots)
      val pipelineGroups =
        index && cores >= resources.executorCores && cores > 1 &&
          queryCount > 1L && limits.groupMaxBytes.forall(
            _ >= SearchResources.groupBytesPerQuery(
              layout,
              k,
              pipelineGroups = true
            )
          )
      Budget(
        resources,
        profile,
        slots,
        cores,
        heap,
        fraction,
        SearchResources.segmentBudget(
          memory.limit,
          heap,
          memory.sparkOffHeap,
          slots,
          limits.segmentsMaxBytes,
          index = index
        ),
        queries,
        SearchResources.groupBytes(
          limits.groupMaxBytes,
          queries.bytes,
          cores,
          SearchResources.groupBytesPerQuery(layout, k, pipelineGroups)
        ),
        pipelineGroups
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
      Math.addExact(
        SearchPlan.PackedCandidateReserveBytes.toLong + 4L,
        rowBytes
      )
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

  /** How many partitions the merge runs in: one for each task slot the job has
    * and for every first-stage task, so no executor sits out the merge; more
    * when the queries would otherwise pile up beyond `queriesPerPartition` in
    * one; never more than there are queries, since a query is one key and
    * cannot be split. The merge is an RDD shuffle rather than a SQL
    * aggregation, so nothing downstream re-partitions it and
    * `spark.sql.shuffle.partitions` does not apply.
    */
  def mergePartitions(
      parallelism: Int,
      queries: Long,
      firstStageTasks: Int,
      queriesPerPartition: Int
  ): Int = {
    require(queries > 0, s"A search needs at least one query: $queries")
    require(
      firstStageTasks > 0,
      s"A search needs at least one task: $firstStageTasks"
    )
    require(
      queriesPerPartition > 0,
      s"A partition takes at least one query: $queriesPerPartition"
    )
    val slots = math.max(1, parallelism)
    val byQueries =
      (queries + queriesPerPartition - 1L) / queriesPerPartition
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
