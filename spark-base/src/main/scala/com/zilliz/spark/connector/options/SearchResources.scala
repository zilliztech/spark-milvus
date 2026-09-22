package com.zilliz.spark.connector.options

/** The sizes of a search that are not the user's to know: the exact scan's base
  * block and what one task may keep, off the heap and on it.
  *
  * One exact-scan call hands the batched distance entry one block of base
  * vectors, and the entry tiles the block itself (4,096 queries by 1,024 rows,
  * decision 27), so the block only sets the fixed cost of a call and the memory
  * a task holds: it is a constant 32 MiB, measured the same per core as 128 MiB
  * (docs/design/architecture/search-resources.html section 3.2). The L3-derived
  * rule that preceded it served the per-query kernel and went with it.
  *
  * A task keeps either its segment set or its queries (section 3.3). Segment
  * data -- vector batches in Arrow, persisted indexes in Knowhere -- is off the
  * heap, in what the executor's limit leaves after the heap and the JVM's own
  * share; queries, their ids and their top-k are on the heap, in the part of it
  * Spark does not manage. The search tasks that run at once on one executor
  * divide both.
  */
object SearchResources {

  final case class Choice(bytes: Long, reason: String)

  /** What the JVM and Spark themselves take off the heap. */
  val JvmReserveBytes: Long = 1L << 30

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
            val offHeap = math.max(0L, limit - heapBytes - JvmReserveBytes)
            val (room, how) =
              if (index)
                (
                  math.max(0L, offHeap - IndexWorkingBytes),
                  s"-${mib(IndexWorkingBytes)}"
                )
              else ((offHeap * OffHeapShare).toLong, s"x$OffHeapShare")
            val raw = room / running
            val perTask = math.max(MinSegmentBudgetBytes, raw)
            val floor = if (raw < MinSegmentBudgetBytes) "floor" else "auto"
            Choice(
              perTask,
              s"limit=${mib(limit)} heap=${mib(heapBytes)} offheap=${mib(offHeap)} " +
                s"$how / $running tasks -> ${mib(perTask)} ($floor)"
            )
        }
    }
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
