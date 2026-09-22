package com.zilliz.spark.connector.options

/** The two sizes of a search that are not the user's to know: the exact scan's
  * base block and the vectors a task keeps resident.
  *
  * One exact-scan call hands the batched distance entry one block of base
  * vectors, and the entry tiles the block itself (4,096 queries by 1,024 rows,
  * decision 27), so the block only sets the fixed cost of a call and the memory
  * a task holds: it is a constant 32 MiB, measured the same per core as 128 MiB
  * (docs/design/architecture/search-resources.html section 3.2). The L3-derived
  * rule that preceded it served the per-query kernel and went with it.
  *
  * A task that answers several query groups keeps its segment set in Arrow's
  * off-heap memory between groups. That memory is what the executor's limit
  * leaves after the heap and the JVM's own share, and half of that is for
  * vectors, the rest for index loads, shuffle and Arrow's other buffers; the
  * tasks of one executor divide it (section 3.3).
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
  val MinVectorsBudgetBytes: Long = 64L << 20

  /** What an executor whose memory limit cannot be read plans against, per
    * executor, as the option's fixed default used to be.
    */
  val FallbackVectorsBytes: Long = 2L << 30

  /** The bytes of vectors one task keeps resident.
    *
    * @param memoryLimitBytes
    *   what the executor may use in all: its cgroup limit or the machine's
    *   total in local mode, the container Spark asked for otherwise
    * @param heapBytes
    *   the executor JVM's maximum heap
    * @param slots
    *   the tasks that run at once on the executor
    * @param configured
    *   `milvus.search.vectors.max.bytes` when the call set it, which is per
    *   executor and is divided by the slots as it is
    */
  def vectorsBudget(
      memoryLimitBytes: Option[Long],
      heapBytes: Long,
      slots: Int,
      configured: Option[Long]
  ): Choice = {
    val tasks = math.max(1, slots)
    configured match {
      case Some(bytes) =>
        val perTask = math.max(1L, bytes / tasks)
        Choice(
          perTask,
          s"$bytes / $tasks slots -> ${mib(perTask)} (option ${MilvusOption.SearchVectorsMaxBytes})"
        )
      case None =>
        memoryLimitBytes match {
          case None =>
            val perTask = math.max(1L, FallbackVectorsBytes / tasks)
            Choice(
              perTask,
              s"memory limit unknown -> ${mib(FallbackVectorsBytes)} / $tasks slots = ${mib(perTask)} (default)"
            )
          case Some(limit) =>
            val offHeap = math.max(0L, limit - heapBytes - JvmReserveBytes)
            val raw = (offHeap * OffHeapShare / tasks).toLong
            val perTask = math.max(MinVectorsBudgetBytes, raw)
            val how = if (raw < MinVectorsBudgetBytes) "floor" else "auto"
            Choice(
              perTask,
              s"limit=${mib(limit)} heap=${mib(heapBytes)} offheap=${mib(
                  offHeap
                )} x$OffHeapShare / $tasks slots -> ${mib(perTask)} ($how)"
            )
        }
    }
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
