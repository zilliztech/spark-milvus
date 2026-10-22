package com.zilliz.spark.connector.search

import scala.util.Try

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.SparkSession

import com.zilliz.milvus.storage.schema.VectorLayout
import com.zilliz.milvus.storage.vector.{MachineResources, SearchPlan}
import com.zilliz.spark.connector.options.{MilvusOption, SearchLimits}

/** The sizes of a search that are not the user's to know: the exact scan's base
  * block and what one task may keep, off the heap and on it.
  *
  * One exact-scan call hands the batched distance entry one block of base
  * vectors, and the entry tiles the block itself (4,096 queries by 1,024 rows),
  * so the block only sets the fixed cost of a call and the memory a task holds:
  * it is a constant 32 MiB, measured the same per core as 128 MiB. The
  * L3-derived rule that preceded it served the per-query kernel and went with
  * it.
  *
  * A task keeps either its segment set or its queries (section 3.3). Segment
  * data -- vector batches in Arrow, persisted indexes in Knowhere -- is off the
  * heap, in what the executor's limit leaves after the heap, Spark's own
  * off-heap pool and the JVM's own share; queries, their ids and their top-k
  * are on the heap, in the part of it Spark does not manage. The search tasks
  * that run at once on one executor divide both.
  */
private[search] object SearchResources {

  final case class Choice(bytes: Long, reason: String)

  /** An executor's memory as the driver can know it.
    *
    * @param limit
    *   what the executor may use in all, when it can be known
    * @param heap
    *   the executor JVM's maximum heap
    * @param sparkOffHeap
    *   the pool Spark manages outside the heap when `spark.memory.offHeap` is
    *   enabled, where its shuffle sort pages live; it is inside the limit but
    *   not available to the segments, so the native room excludes it
    */
  final case class ExecutorMemory(
      limit: Option[Long],
      heap: Long,
      sparkOffHeap: Long
  )

  /** The executor's memory: its limit, when it can be known, its heap and
    * Spark's off-heap pool. In local mode the executor is this JVM, so the
    * limit is the cgroup's or the machine's and the heap is this runtime's. On
    * a cluster the driver cannot read the executor's cgroup, but the container
    * Spark asked for is its limit: `spark.executor.memory` plus the overhead
    * and, when enabled, the off-heap size.
    */
  def executorMemory(spark: SparkSession): ExecutorMemory = {
    val master = spark.sparkContext.master
    def bytes(key: String, default: String): Long =
      JavaUtils.byteStringAsBytes(spark.conf.get(key, default))
    val sparkOffHeap =
      if (spark.conf.get("spark.memory.offHeap.enabled", "false") == "true")
        bytes("spark.memory.offHeap.size", "0")
      else 0L
    if (master.startsWith("local[") || master == "local") {
      ExecutorMemory(
        MachineResources.memoryLimitBytes(),
        Runtime.getRuntime.maxMemory(),
        sparkOffHeap
      )
    } else {
      val heap = memoryMiB(spark.conf.get("spark.executor.memory", "1g"))
      val overhead =
        spark.conf.getOption("spark.executor.memoryOverhead") match {
          case Some(set) => memoryMiB(set)
          case None =>
            val factor = spark.conf
              .getOption("spark.executor.memoryOverheadFactor")
              .flatMap(value => Try(value.trim.toDouble).toOption)
              .getOrElse(0.10)
            math.max(384L << 20, (heap * factor).toLong)
        }
      ExecutorMemory(Some(heap + overhead + sparkOffHeap), heap, sparkOffHeap)
    }
  }

  /** Spark's executor heap and overhead options default to MiB. */
  private[search] def memoryMiB(value: String): Long =
    Math.multiplyExact(JavaUtils.byteStringAsMb(value), 1L << 20)

  /** `spark.network.maxRemoteBlockSizeFetchToMem`: the largest shuffle block
    * the reduce side fetches into memory rather than to disk, which is heap a
    * merge task holds while it reads an answer's block.
    */
  def fetchToMemoryBytes(spark: SparkSession): Long =
    Try(
      JavaUtils.byteStringAsBytes(
        spark.conf.get(
          "spark.network.maxRemoteBlockSizeFetchToMem",
          SearchMerge.FetchToMemoryBytes.toString
        )
      )
    ).getOrElse(SearchMerge.FetchToMemoryBytes)

  /** `spark.memory.fraction`: the share of the heap Spark manages, the rest
    * being where a task's own objects live.
    */
  def memoryFraction(spark: SparkSession): Double = spark.conf
    .getOption("spark.memory.fraction")
    .flatMap(value => Try(value.trim.toDouble).toOption)
    .getOrElse(0.6)

  /** What the JVM and Spark themselves take off the heap, at least: metaspace,
    * thread stacks, GC and JIT data, Netty's direct buffers, the shuffle
    * client's. Measured 1.3-1.4 GB by native memory tracking on the P3
    * executors (6 GiB heap), where the fixed 1 GiB this used to be left the
    * plan running at 27.5 of 30 GiB.
    */
  val MinJvmReserveBytes: Long = 2L << 30

  /** The reserve's share of the memory limit above the minimum, and its cap: a
    * bigger executor runs more threads and buffers, but not without end.
    */
  val JvmReserveShare: Double = 0.08
  val MaxJvmReserveBytes: Long = 8L << 30

  /** The reserve taken off `memoryLimitBytes` before the off-heap room is
    * divided: `max(2 GiB, min(8% of the limit, 8 GiB))`.
    */
  def jvmReserveBytes(memoryLimitBytes: Long): Long =
    math.max(
      MinJvmReserveBytes,
      math.min((memoryLimitBytes * JvmReserveShare).toLong, MaxJvmReserveBytes)
    )

  /** The share of the off-heap room the resident vectors may take. */
  val OffHeapShare: Double = 0.5

  /** The least a task plans against, however small the room: below this the
    * plan would cut segments into more sets than there are row groups to read,
    * and a task that overruns it streams anyway.
    */
  val MinSegmentBudgetBytes: Long = 64L << 20

  /** What an executor whose memory limit cannot be read plans against, per
    * executor, as the option's fixed default used to be.
    */
  val FallbackSegmentBytes: Long = 2L << 30

  /** What an index search leaves off the heap besides the indexes it keeps and
    * the one it loads: shuffle fetch buffers and Knowhere's working memory.
    */
  val IndexWorkingBytes: Long = 1L << 30

  /** The heap Spark reserves for itself before it divides the rest, as
    * `UnifiedMemoryManager` does.
    */
  val ReservedHeapBytes: Long = 300L << 20

  /** The segment data one task keeps off the heap: vector batches in exact
    * mode, persisted indexes in index mode.
    *
    * @param memoryLimitBytes
    *   what the executor may use in all: its cgroup limit or the machine's
    *   total in local mode, the container Spark asked for otherwise
    * @param heapBytes
    *   the executor JVM's maximum heap
    * @param sparkOffHeapBytes
    *   Spark's own off-heap pool, inside the limit and not the segments' to
    *   use: its shuffle sort pages fill it while the first stage writes its
    *   candidates
    * @param tasks
    *   the search tasks that run at once on the executor
    * @param configured
    *   `milvus.search.segments.max.bytes` when the call set it, which is per
    *   executor and is divided by the tasks as it is
    * @param index
    *   true for an index search, which accounts its loads itself and keeps the
    *   off-heap room less [[IndexWorkingBytes]]; an exact scan keeps half the
    *   room for its read buffers
    */
  def segmentBudget(
      memoryLimitBytes: Option[Long],
      heapBytes: Long,
      sparkOffHeapBytes: Long,
      tasks: Int,
      configured: Option[Long],
      index: Boolean
  ): Choice = {
    val running = math.max(1, tasks)
    configured match {
      case Some(bytes) =>
        val perTask = math.max(1L, bytes / running)
        Choice(
          perTask,
          s"$bytes / $running tasks -> ${mib(perTask)} (option ${MilvusOption.SearchSegmentsMaxBytes})"
        )
      case None =>
        memoryLimitBytes match {
          case None =>
            val perTask = math.max(1L, FallbackSegmentBytes / running)
            Choice(
              perTask,
              s"memory limit unknown -> ${mib(FallbackSegmentBytes)} / $running tasks = ${mib(perTask)} (default)"
            )
          case Some(limit) =>
            val reserve = jvmReserveBytes(limit)
            val native = math.max(
              0L,
              limit - heapBytes - math.max(0L, sparkOffHeapBytes) - reserve
            )
            val (room, how) =
              if (index)
                (
                  math.max(0L, native - IndexWorkingBytes),
                  s"-${mib(IndexWorkingBytes)}"
                )
              else ((native * OffHeapShare).toLong, s"x$OffHeapShare")
            val raw = room / running
            val perTask = math.max(MinSegmentBudgetBytes, raw)
            val floor = if (raw < MinSegmentBudgetBytes) "floor" else "auto"
            Choice(
              perTask,
              s"limit=${mib(limit)} heap=${mib(heapBytes)} " +
                s"sparkOffHeap=${mib(math.max(0L, sparkOffHeapBytes))} " +
                s"reserve=${mib(reserve)} native=${mib(native)} $how / " +
                s"$running tasks -> ${mib(perTask)} ($floor)"
            )
        }
    }
  }

  /** The heap Spark manages, `(heap - 300 MiB) x spark.memory.fraction`,
    * divided by the tasks that run at once on the executor: the fair share of
    * the execution pool one task's reduce-side map may grow to before Spark
    * spills it. The merge's retained candidate runs live in that map.
    */
  def executionBudget(
      heapBytes: Long,
      memoryFraction: Double,
      tasks: Int
  ): Choice = {
    val running = math.max(1, tasks)
    val managed =
      (math.max(0L, heapBytes - ReservedHeapBytes) * memoryFraction).toLong
    val perTask = math.max(0L, managed / running)
    Choice(
      perTask,
      s"heap=${mib(heapBytes)} x $memoryFraction / $running tasks -> ${mib(perTask)}"
    )
  }

  /** The queries one task keeps on the heap: the part of the heap Spark does
    * not manage, `(heap - 300 MiB) x (1 - spark.memory.fraction)`, divided by
    * the search tasks that run at once on the executor.
    */
  def queryBudget(
      heapBytes: Long,
      memoryFraction: Double,
      tasks: Int
  ): Choice = {
    val running = math.max(1, tasks)
    val user =
      (math.max(
        0L,
        heapBytes - ReservedHeapBytes
      ) * (1.0 - memoryFraction)).toLong
    val perTask = math.max(0L, user / running)
    Choice(
      perTask,
      s"heap=${mib(heapBytes)} x (1 - $memoryFraction) / $running tasks -> ${mib(perTask)}"
    )
  }

  /** Queries one Knowhere call answers for every core the search task holds,
    * when the heap allows: below a few queries a core the call's fixed cost
    * (packing the matrix, the JNI round trip, collecting the candidates on the
    * JVM side) is what the task spends its time on. The executor scaling test
    * of 2026-10-08 measured 13 queries a call on 32 cores at k=100,000 (the 64
    * MiB group of the harness): search-stage CPU 35% on L x 4, 77% on L x 16 at
    * k=10,000 against 92% on 8-core executors.
    */
  val GroupQueriesPerCore: Int = 8

  /** The share of a task's query heap budget one group may take: the rest holds
    * the answers the task keeps across groups and the rows they become.
    */
  val GroupHeapShare: Double = 0.5

  /** The most a derived group takes, whatever the heap: Knowhere writes a
    * call's answers into Arrow buffers off the heap, queries x k x 12 bytes a
    * segment, and a 4 GiB group of 8,800 queries at k=10,000 on a 24 GiB-heap
    * executor exhausted the JVM's direct memory (2026-10-13, exit 52) where a 2
    * GiB group ran. The explicit option is not capped.
    */
  val MaxDerivedGroupBytes: Long = 2L << 30

  /** The current merger and packed output can overlap the next merger. Two
    * query groups also retain their vectors, ids and queue metadata. Native
    * query matrices do not overlap: search closes one before returning its
    * merger and starting the next group. Spark manages shuffle pages
    * separately.
    */
  def groupBytesPerQuery(
      layout: VectorLayout,
      k: Int,
      pipelineGroups: Boolean
  ): Long =
    if (pipelineGroups)
      2L * (layout.rowBytes.toLong + SearchPlan.QueryIdBytes + SearchPlan.TopKQueueBytes) +
        3L * k * SearchPlan.PackedCandidateReserveBytes
    else SearchPlan.bytesPerQuery(layout, k)

  /** Convert the execution budget into the equivalent limit for core's
    * single-group algorithm, preserving the number of queries, including an
    * explicit option too small for one overlapping query pair.
    */
  def groupPlanBytes(
      bytes: Long,
      layout: VectorLayout,
      k: Int,
      pipelineGroups: Boolean
  ): Long = {
    if (!pipelineGroups) return bytes
    val perQuery = groupBytesPerQuery(layout, k, pipelineGroups)
    require(
      bytes >= perQuery,
      s"${MilvusOption.SearchGroupMaxBytes}=$bytes cannot hold one query " +
        s"with overlapping search and output ($perQuery bytes)"
    )
    (bytes / perQuery) * SearchPlan.bytesPerQuery(layout, k)
  }

  /** How many bytes of queries one Knowhere call takes: the explicit
    * `milvus.search.group.max.bytes` as it is, otherwise the larger of the old
    * 512 MiB default and [[GroupQueriesPerCore]] queries for every core the
    * task holds, capped at [[GroupHeapShare]] of the task's query budget and
    * never below one query.
    *
    * @param configured
    *   the option when the call set it
    * @param queryBudgetBytes
    *   what the task may keep on the heap ([[queryBudget]])
    * @param cores
    *   the cores the search task holds
    * @param perQueryBytes
    *   what one query costs a group: its row and its bounded top-k
    */
  def groupBytes(
      configured: Option[Long],
      queryBudgetBytes: Long,
      cores: Int,
      perQueryBytes: Long
  ): Choice = configured match {
    case Some(bytes) =>
      Choice(bytes, s"$bytes (option ${MilvusOption.SearchGroupMaxBytes})")
    case None =>
      val one = math.max(1L, perQueryBytes)
      val wanted = GroupQueriesPerCore.toLong * math.max(1, cores) * one
      val cap = math.max(
        one,
        math.min(
          MaxDerivedGroupBytes,
          (math.max(0L, queryBudgetBytes) * GroupHeapShare).toLong
        )
      )
      val bytes =
        math.min(cap, math.max(SearchLimits.DefaultGroupMaxBytes, wanted))
      val how =
        if (
          bytes == cap && cap < math.max(
            SearchLimits.DefaultGroupMaxBytes,
            wanted
          )
        ) (if (cap == MaxDerivedGroupBytes) "2 GiB cap" else "heap cap")
        else if (wanted > SearchLimits.DefaultGroupMaxBytes)
          s"$GroupQueriesPerCore queries x $cores cores"
        else "default"
      Choice(
        bytes,
        s"${mib(bytes)} = ${bytes / one} queries of ${one}B ($how; budget ${mib(queryBudgetBytes)} x $GroupHeapShare)"
      )
  }

  /** The base block one exact-scan call takes: the batched distance entry tiles
    * it internally, so 32 MiB and 128 MiB measure the same per core and the
    * block only sets the fixed cost per call and a task's memory.
    */
  val DefaultBatchBytes: Long = 32L << 20

  /** @param configured
    *   the value of `milvus.read.batch.max.bytes` when the call set it, which
    *   is then used as it is
    */
  def exactScanBatch(configured: Option[Long]): Choice = configured match {
    case Some(bytes) =>
      Choice(bytes, s"$bytes (option ${MilvusOption.ReadBatchMaxBytes})")
    case None =>
      Choice(DefaultBatchBytes, s"${mib(DefaultBatchBytes)} (default)")
  }

  private def mib(bytes: Long): String =
    if (bytes >= (1L << 30) && bytes % (1L << 30) == 0) s"${bytes >> 30}GiB"
    else if (bytes >= (1L << 30))
      f"${bytes / (1024.0 * 1024.0 * 1024.0)}%.1fGiB"
    else if (bytes % (1L << 20) == 0) s"${bytes >> 20}MiB"
    else if (bytes >= (1L << 20)) f"${bytes / (1024.0 * 1024.0)}%.1fMiB"
    else s"${bytes}B"
}
